-- Nhánh CHECKPOINT của opening_balance — xem placeholder tương ứng trong
-- query.sql (opening_balance AS (...)) và config.SoChiTietVatLieuOpeningBalanceUseCheckpoint
-- (bật bằng SO_CHI_TIET_VAT_LIEU_OPENING_BALANCE_SOURCE=checkpoint).
--
-- Thay vì cộng dồn TOÀN BỘ lịch sử PostedDate < FROM_DATE (nhánh "raw"), dùng
-- checkpoint tháng gần nhất < tháng(FROM_DATE) từ eb.repository_ledger_checkpoint
-- rồi chỉ cộng thêm "phần lẻ" raw phát sinh SAU tháng checkpoint đó.
--
-- KHÔNG có guard so covered_upto_ts (thiết kế mục 7.1) ở bản này — nếu Flink
-- checkpoint job trễ/chết, nhánh này ÂM THẦM thiếu phát sinh mới nhất thay vì
-- tự rơi về quét raw. Chấp nhận được cho mục đích so sánh/thử nghiệm qua env,
-- KHÔNG dùng làm mặc định ở PROD khi chưa có guard.
--
-- MaxPreDate/MaxMainUnitPrice (chỉ dùng hiển thị dòng "Số dư đầu kỳ", không
-- ảnh hưởng NetQuantity/NetAmount) rơi về sentinel epoch/0 khi key có checkpoint
-- nhưng không còn phát sinh "phần lẻ" nào — xem coalesce/ifNull cuối cùng.
--
-- requested_keys: eb.repository_ledger_checkpoint khong co cot de ap truc
-- tiep cac filter kho/vat tu/loai gas nghiep vu cua ledger_scope (khac ten
-- cot, PascalCase vs lowercase) - nen loc ckpt_latest theo dung tap key DA
-- QUA het cac filter do, bang cach lay DISTINCT key tu chinh ledger_scope.
-- THIEU dong nay se lam opening_balance tra ve TAT CA repository/material
-- cua company, khong chi may key duoc yeu cau.
WITH requested_keys AS
(
    SELECT DISTINCT RepositoryID, MaterialGoodsID
    FROM ledger_scope
),
ckpt_latest AS
(
    SELECT
        k.repository_id AS repository_id, k.material_goods_id AS material_goods_id,
        argMax(k.cumulative_qty, k.month)    AS ckpt_qty,
        argMax(k.cumulative_amount, k.month) AS ckpt_amount,
        max(k.month)                         AS ckpt_month
    FROM eb.repository_ledger_checkpoint AS k
    INNER JOIN requested_keys AS rk
        ON k.repository_id = rk.RepositoryID AND k.material_goods_id = rk.MaterialGoodsID
    WHERE k.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
      AND k.month < toYYYYMM(toDateTime('{{FROM_DATE}}'))
    GROUP BY k.repository_id, k.material_goods_id
),
tail AS
(
    SELECT
        l.RepositoryID    AS RepositoryID,
        l.MaterialGoodsID AS MaterialGoodsID,
        sum(if(l.UnitID = l.MainUnitID, ifNull(l.IWQuantity, 0), ifNull(l.MainIWQuantity, 0))
          - if(l.UnitID = l.MainUnitID, ifNull(l.OWQuantity, 0), ifNull(l.MainOWQuantity, 0))) AS NetQuantity,
        sum(ifNull(l.IWAmount, 0) - ifNull(l.OWAmount, 0)) AS NetAmount,
        max(l.Date)          AS MaxPreDate,
        max(l.MainUnitPrice) AS MaxMainUnitPrice
    FROM ledger_scope AS l
    LEFT JOIN ckpt_latest AS c
        ON l.RepositoryID = c.repository_id AND l.MaterialGoodsID = c.material_goods_id
    WHERE l.PostedDate < toDateTime('{{FROM_DATE}}')
      -- key chưa từng có checkpoint (c.ckpt_month NULL) -> quét từ đầu lịch sử,
      -- key đã có checkpoint -> chỉ quét phần SAU tháng checkpoint đó.
      AND (c.ckpt_month IS NULL OR toYYYYMM(l.PostedDate) > c.ckpt_month)
    GROUP BY l.RepositoryID, l.MaterialGoodsID
)
SELECT
    coalesce(t.RepositoryID, c.repository_id)        AS RepositoryID,
    coalesce(t.MaterialGoodsID, c.material_goods_id) AS MaterialGoodsID,
    ifNull(c.ckpt_qty, 0) + ifNull(t.NetQuantity, 0)  AS NetQuantity,
    ifNull(c.ckpt_amount, 0) + ifNull(t.NetAmount, 0) AS NetAmount,
    coalesce(t.MaxPreDate, toDateTime('1970-01-01 00:00:00')) AS MaxPreDate,
    ifNull(t.MaxMainUnitPrice, 0)                     AS MaxMainUnitPrice
FROM ckpt_latest AS c
FULL OUTER JOIN tail AS t
    ON c.repository_id = t.RepositoryID AND c.material_goods_id = t.MaterialGoodsID

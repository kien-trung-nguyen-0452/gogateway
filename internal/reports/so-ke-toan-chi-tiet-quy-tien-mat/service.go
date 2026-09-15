// Package so_ke_toan_chi_tiet_quy_tien_mat — Sổ kế toán chi tiết quỹ tiền mặt.
//
// Nguồn tham chiếu: Proc_SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT (1.696 dòng).
//
// BA KHÁC BIỆT SO VỚI SỔ CHI TIẾT TÀI KHOẢN
// -----------------------------------------
//
//  1. SoTon luỹ kế — cộng dồn theo từng tài khoản. Proc gốc dùng thủ thuật
//     UPDATE ... SET @var của SQL Server, vốn KHÔNG được đảm bảo về thứ tự và
//     chỉ chạy đúng nhờ clustered index. Ở đây thứ tự do ORDER BY bảo đảm.
//
//  2. Tách Nợ/Có thành phiếu thu / phiếu chi — làm ở SQL bằng UNION ALL, KHÔNG
//     làm ở Go. Xem ghi chú trong query.sql: CAType nằm trong ORDER BY trước
//     số hiệu chứng từ, nên trong cùng một ngày mọi phiếu thu phải chạy trước
//     mọi phiếu chi. Tách ở Go sẽ phát hai dòng liền nhau và sai thứ tự.
//
//  3. Hai cặp cột tiền tệ CỐ ĐỊNH vai trò: PhatSinhNo/Co luôn nguyên tệ,
//     PhatSinhNoQD/CoQD luôn quy đổi. Frontend render cả bốn cột cùng lúc nên
//     KHÔNG cột nào đổi theo typeShowCurrency — xem buildDetailQuery().
//
// Proc gốc lặp lại gần như y hệt bốn lần cho tổ hợp (loại tiền × gộp). Ở đây
// gộp thành hai tham số.
//
// ── SAO CHÉP CÓ Ý THỨC MỘT BẤT NHẤT CỦA PROC ────────────────────────────────
// Tồn đầu kỳ dùng nguyên tệ cho SoTon bất kể typeShowCurrency, trong khi dòng
// chi tiết thì đổi theo tham số. Xem ghi chú đầy đủ ở query_opening.sql. Giữ
// nguyên để khớp OLTP; dev sau quyết định có sửa không.
//
// FALLBACK STAGING — giống so-chi-tiet-cac-tai-khoan
// ---------------------------------------------------
// useStaging bật đọc từ eb_staging.stg_general_ledger(_detail) thay vì
// eb_dwh.fact_gl_entry_line khi pipeline fact bị lỗi, không cần deploy lại
// code. Xem staging_source.sql và config.SoKeToanChiTietQuyTienMatUseStaging
// (bật bằng SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE=staging).
package so_ke_toan_chi_tiet_quy_tien_mat

import (
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

//go:embed query.sql
var queryTemplate string

//go:embed query_opening.sql
var openingTemplate string

//go:embed staging_source.sql
var stagingSourceExpr string

// factSourceExpr là giá trị {{GL_SOURCE}} mặc định — đọc thẳng fact table.
const factSourceExpr = "eb_dwh.fact_gl_entry_line AS f  FINAL"

var (
	uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

	// Số hiệu tài khoản CÓ THỂ chứa khoảng trắng và gạch ngang: "154 - TT" là
	// một tài khoản THẬT. Dùng danh sách CHẶN thay vì CHO PHÉP.
	accountPattern = regexp.MustCompile(`^[^'"\\\x00-\x1f]{1,50}$`)

	currencyPattern = regexp.MustCompile(`^[0-9A-Za-z]{2,10}$`)
	clusterPattern  = regexp.MustCompile(`^[0-9A-Za-z._-]{1,50}$`)
)

type Service struct {
	conn clickhouse.Conn

	// useStaging: true = doc tu eb_staging.stg_general_ledger(_detail) thay vi
	// eb_dwh.fact_gl_entry_line - xem staging_source.sql va
	// config.SoKeToanChiTietQuyTienMatUseStaging. Dung tam thoi khi pipeline
	// fact loi, giong so-chi-tiet-cac-tai-khoan.
	useStaging bool
}

func NewService(conn clickhouse.Conn, useStaging bool) *Service {
	return &Service{conn: conn, useStaging: useStaging}
}

type driverRows interface {
	Scan(dest ...any) error
}

type QueryParams struct {
	CompanyIDs       []string // đã swap UUID phía Java
	PrimaryCompanyID string
	AccountNumbers   []string
	FromDate         string // YYYY-MM-DD
	ToDate           string // YYYY-MM-DD
	CurrencyID       string // "TH" = tất cả

	// 0 = sổ tài chính (no_fbook), 1 = sổ quản trị (no_mbook).
	// CHÚ Ý: proc này nhận thẳng @typeLedger nên 0 mới là sổ tài chính —
	// ngược chiều với isFinancialBook của module so_chi_tiet_tai_khoan.
	TypeLedger int

	// 0 = hiển thị số quy đổi (debit_amount)
	// 1 = hiển thị số nguyên tệ (debit_amount_original)
	TypeShowCurrency int

	GroupTheSameItem int
	IsDependent      int
	ClusterID        string
}

// =============================================================================
// ENTRYPOINT
// =============================================================================

// GetSoQuy gom toàn bộ kết quả vào slice. Chỉ dùng cho báo cáo nhỏ.
func (s *Service) GetSoQuy(ctx context.Context, p QueryParams) ([]Row, int64, error) {
	var result []Row
	elapsedMs, err := s.StreamSoQuy(ctx, p, func(row Row) error {
		result = append(result, row)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return result, elapsedMs, nil
}

// StreamSoQuy gọi onRow cho TỪNG dòng ngay khi dựng xong.
func (s *Service) StreamSoQuy(
	ctx context.Context, p QueryParams, onRow func(Row) error,
) (int64, error) {
	if err := validateParams(p); err != nil {
		return 0, err
	}
	start := time.Now()

	noCol := "no_fbook"
	if p.TypeLedger == 1 {
		noCol = "no_mbook"
	}

	companyIDs, err := s.resolveCompanyScope(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("resolve company scope: %w", err)
	}

	accounts := cleanAccounts(p.AccountNumbers)
	if len(accounts) == 0 {
		return 0, fmt.Errorf("khong con tai khoan nao sau khi loc")
	}
	for _, a := range accounts {
		if !accountPattern.MatchString(a) {
			return 0, fmt.Errorf("so hieu tai khoan khong hop le: %q", a)
		}
	}

	// LUON mo rong xuong tai khoan con — khong co co bat/tat.
	//
	// Ban JDBC cu lam viec nay o Java (DynamicReportQuyServiceImpl goi
	// AccountListService.getListChildAccount) roi moi truyen danh sach vao proc.
	// Chuyen sang day de bo round-trip SQL Server cuoi cung con sot trong luong
	// nay — DWH da co bridge_account_hierarchy.
	//
	// Danh sach sau khi mo rong chua CA cha lan con (xem expandAccounts), nen
	// ton dau ky cua tai khoan cha phai cong don theo cay — viec do do
	// rollupOpeningBalances() lam ngay ben duoi.
	accounts, err = s.expandAccounts(ctx, p.PrimaryCompanyID, accounts)
	if err != nil {
		return 0, fmt.Errorf("expand accounts: %w", err)
	}

	// Loc theo LOAI TIEN cua tai khoan — khac han currencyFilter() von loc theo
	// loai tien cua giao dich. Proc ap ca hai. Xem filterAccountsByCurrency().
	accounts, err = s.filterAccountsByCurrency(ctx, p.PrimaryCompanyID, accounts, p)
	if err != nil {
		return 0, fmt.Errorf("filter accounts by currency: %w", err)
	}
	if len(accounts) == 0 {
		return 0, fmt.Errorf("khong con tai khoan nao sau khi loc theo loai tien")
	}

	sddkMap, err := s.loadOpeningBalances(ctx, companyIDs, accounts, p)
	if err != nil {
		return 0, fmt.Errorf("load opening balances: %w", err)
	}

	// Cong don theo cay: tai khoan cha ("111") phai gom ton dau ky ca nhanh con.
	// PHAI chay sau loadOpeningBalances va truoc khi dung sddkMap.
	//
	// descendants dung tiep cho walker: dong Cong nhom cua tai khoan cha lay
	// SoTon = TONG SoTon cac dong Cong nhom con (xem walker.closeAccount).
	sddkMap, descendants, err := s.rollupOpeningBalances(ctx, p.PrimaryCompanyID, accounts, sddkMap)
	if err != nil {
		return 0, fmt.Errorf("rollup opening balances: %w", err)
	}

	w := &walker{
		onRow:            onRow,
		sddkMap:          sddkMap,
		accounts:         accounts,
		descendants:      descendants,
		closingSoTon:     make(map[string]balance, len(accounts)),
		closingSlot:      make(map[string]int, len(accounts)),
		seen:             make(map[string]bool, len(accounts)),
		typeShowCurrency: p.TypeShowCurrency,
	}

	sql := buildDetailQuery(companyIDs, accounts, p, noCol, s.useStaging)
	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		d, err := scanRow(rows)
		if err != nil {
			return 0, fmt.Errorf("scan error: %w", err)
		}
		w.push(d)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %w", err)
	}

	if err := w.finish(); err != nil {
		return 0, fmt.Errorf("loi ket thuc: %w", err)
	}

	return time.Since(start).Milliseconds(), nil
}

// =============================================================================
// WALKER — máy trạng thái một lượt, có số dư luỹ kế
// =============================================================================

type walker struct {
	onRow            func(Row) error
	sddkMap          map[string]balance
	accounts         []string
	seen             map[string]bool
	typeShowCurrency int

	current string

	// next là vị trí kế tiếp trong accounts chưa được phát ra. Dùng để chèn các
	// tài khoản không có phát sinh vào ĐÚNG CHỖ thay vì dồn xuống cuối.
	//
	// Chạy tiến một chiều: luồng SQL đã ORDER BY theo indexOf(accounts) nên
	// account của dòng sau không bao giờ đứng trước account của dòng trước.
	next int

	// running / runningQD là SoTon luỹ kế.
	//
	// Proc gốc (dòng 1324):
	//     SET @ClosingQuantity = CASE
	//         WHEN RefID IS NULL THEN SoTon                    -- dòng tồn đầu kỳ
	//         WHEN @AccountNumber <> Account THEN PhatSinhNo - PhatSinhCo
	//         ELSE @ClosingQuantity + PhatSinhNo - PhatSinhCo
	//     END
	//
	// Nhánh giữa thực chất là CODE CHẾT: dòng tồn đầu kỳ (PositionOrder = 0)
	// luôn đứng đầu mỗi tài khoản và đã đặt @AccountNumber, nên điều kiện đổi
	// tài khoản không bao giờ đúng ở dòng chi tiết. openAccount() ở đây luôn
	// phát dòng tồn đầu kỳ nên tương đương.
	running   decimal.Decimal
	runningQD decimal.Decimal

	// Cộng dồn phát sinh của tài khoản hiện tại, cho dòng Cộng nhóm.
	tc balance

	// ── ROLLUP SoTon CHO TÀI KHOẢN CHA ──────────────────────────────────────
	// descendants: tài khoản -> hậu duệ (trong phạm vi báo cáo). Tài khoản có
	// hậu duệ = node cha; nó KHÔNG có bút toán hạch toán trực tiếp nên không
	// bao giờ xuất hiện trong luồng SQL.
	//
	// Proc gốc trả PhatSinhNo/Co = NULL và SoTon = tồn đầu kỳ cho các dòng
	// Cộng nhóm này (đã kiểm chứng bằng EXEC proc trực tiếp). Con số người dùng
	// thấy trên web do JAVA tính lại: DynamicReportQuyServiceImpl dòng 501-508
	// ghi đè SoTon/SoTonQD của dòng Cộng nhóm cha bằng TỔNG SoTon/SoTonQD của
	// các dòng Cộng nhóm CON.
	//
	// Kiểm chứng bằng số liệu thật:
	//     1111:  593.433.786.727,69
	//     1112:      561.706.518,65
	//     1113:      -60.300.360,00
	//     ────────────────────────────
	//     111:   593.935.192.886,34  ← khớp đúng web OLTP
	//
	// closingSoTon lưu SoTon cuối kỳ của từng tài khoản LÁ ngay khi closeAccount()
	// phát dòng Cộng nhóm của nó, để cộng dồn cho cha sau.
	descendants  map[string][]string
	closingSoTon map[string]balance

	// pendingParents giữ các tài khoản cha đã tới lượt hiển thị nhưng CHƯA biết
	// SoTon — reserve sẵn slot trong buf (closingSlot), patch lại ở
	// patchParents() khi đã biết SoTon mọi con.
	pendingParents []string

	// ── BUFFER: GIỮ ĐÚNG VỊ TRÍ HIỂN THỊ CỦA TÀI KHOẢN CHA ──────────────────
	// Đúng cách luồng OLTP làm: DynamicReportQuyServiceImpl giữ TOÀN BỘ kết quả
	// proc trong một List rồi mới setSoTon() sửa tại chỗ dòng Cộng nhóm của cha
	// (dòng 501-508) — không phải true streaming. DWH áp dụng y hệt: mọi Row đi
	// qua emit() vào buf thay vì gọi onRow() ngay; StreamSoQuy chỉ gọi onRow()
	// thật SAU KHI walker.finish() xong, qua flush().
	//
	// Đổi lại "gửi ngay từng dòng" lấy đúng CẢ vị trí (111 đứng đầu, khớp OLTP)
	// LẪN số liệu. Chấp nhận được: báo cáo quỹ tiền mặt cỡ vài nghìn dòng.
	buf []Row

	// closingSlot: account (CHỈ tài khoản cha) -> index trong buf của dòng Cộng
	// nhóm cần patch lại SoTon ở patchParents(). Dòng Tồn đầu kỳ KHÔNG cần patch
	// — nó đã đúng ngay từ đầu nhờ sddkMap (rollupOpeningBalances).
	closingSlot map[string]int
}

// emit ghi Row vào buffer theo đúng thứ tự gọi, thay cho onRow() trực tiếp.
// Trả về index vừa ghi để patch lại sau (dòng Tồn đầu kỳ/Cộng nhóm của cha).
func (w *walker) emit(r Row) int {
	w.buf = append(w.buf, r)
	return len(w.buf) - 1
}

// flush gọi w.onRow() cho TOÀN BỘ buffer theo đúng thứ tự — CHỈ gọi một lần,
// ở cuối StreamSoQuy sau khi walker.finish() đã patch xong mọi slot.
func (w *walker) flush() error {
	for _, r := range w.buf {
		if err := w.onRow(r); err != nil {
			return err
		}
	}
	return nil
}

// push xử lý một dòng đã tách sẵn thu/chi từ SQL.
//
// Không tách ở đây — xem ghi chú đầu package và trong query.sql.
func (w *walker) push(d rawRow) {
	if d.Account != w.current {
		if w.current != "" {
			w.closeAccount()
		}
		// Phát các tài khoản KHÔNG có phát sinh nằm TRƯỚC tài khoản sắp mở, để
		// chúng ra đúng vị trí trong thứ tự người dùng chọn.
		//
		// Điển hình là tài khoản CHA ("111"): nó chỉ là node gộp, không có bút
		// toán hạch toán trực tiếp nên không bao giờ xuất hiện trong luồng SQL.
		// Nếu để finish() quét bù thì "111" bị dồn xuống CUỐI báo cáo, trong khi
		// proc đặt nó ngay đầu — ORDER BY của proc là indexOf(danh sách chọn).
		w.flushEmptyBefore(d.Account)
		w.openAccount(d.Account)
	}

	// Số dư luỹ kế: cộng thu, trừ chi.
	w.running = w.running.Add(d.PhatSinhNo).Sub(d.PhatSinhCo)
	w.runningQD = w.runningQD.Add(d.PhatSinhNoQD).Sub(d.PhatSinhCoQD)

	w.tc.add(balance{
		Debit:    d.PhatSinhNo,
		Credit:   d.PhatSinhCo,
		DebitQD:  d.PhatSinhNoQD,
		CreditQD: d.PhatSinhCoQD,
	})

	w.emit(Row{
		PositionOrder:        PositionDetail,
		CAType:               d.CAType,
		OrderPriority:        d.OrderPriority,
		RefID:                d.RefID,
		TypeID:               d.TypeID,
		Date:                 d.VoucherDate,
		PostedDate:           d.PostedDate,
		ReceiptRefNo:         d.ReceiptRefNo,
		PaymentRefNo:         d.PaymentRefNo,
		JournalMemo:          d.JournalMemo,
		Reason:               d.Reason,
		Account:              d.Account,
		AccountCorresponding: d.AccountCorresponding,
		PhatSinhNo:           D(d.PhatSinhNo),
		PhatSinhCo:           D(d.PhatSinhCo),
		PhatSinhNoQD:         D(d.PhatSinhNoQD),
		PhatSinhCoQD:         D(d.PhatSinhCoQD),
		SoTon:                D(w.running),
		SoTonQD:              D(w.runningQD),
		ExchangeRate:         D(d.ExchangeRate),
		AccountingObjectCode: d.AccountingObjectCode,
		AccountingObjectName: d.AccountingObjectName,
		CustomField:          d.CustomField,
		CustomFieldDetail:    d.CustomFieldDetail,
	})
}

// openAccount phát dòng Tồn đầu kỳ và đặt lại bộ đếm luỹ kế.
//
// Tồn đầu kỳ của tài khoản CHA đã đúng ngay từ đầu nhờ sddkMap
// (rollupOpeningBalances cộng dồn theo cây) — không cần patch lại như dòng
// Cộng nhóm.
func (w *walker) openAccount(acc string) {
	w.current = acc
	w.seen[acc] = true
	w.tc = balance{}

	b := w.sddkMap[acc] // zero value nếu không có

	// SoTon dùng nguyên tệ, SoTonQD dùng quy đổi — BẤT KỂ typeShowCurrency.
	// Đây là bất nhất của proc gốc, sao chép có ý thức để khớp OLTP.
	// Xem ghi chú đầy đủ ở query_opening.sql.
	w.running = b.Debit.Sub(b.Credit)
	w.runningQD = b.DebitQD.Sub(b.CreditQD)

	// Proc gốc LUÔN phát dòng tồn đầu kỳ, kể cả bằng 0 — khác sổ chi tiết tài
	// khoản vốn bỏ qua khi bằng 0. Cột SoTon cần một mốc bắt đầu.
	w.emit(Row{
		PositionOrder: PositionOpening,
		Account:       acc,
		JournalMemo:   "Số tồn đầu kỳ",
		Reason:        "Số tồn đầu kỳ",
		PhatSinhNo:    D(decimal.Zero),
		PhatSinhCo:    D(decimal.Zero),
		PhatSinhNoQD:  D(decimal.Zero),
		PhatSinhCoQD:  D(decimal.Zero),
		SoTon:         D(w.running),
		SoTonQD:       D(w.runningQD),
		ExchangeRate:  D(decimal.Zero),
	})
}

// closeAccount phát dòng Cộng nhóm.
//
// SoTon của dòng này là số dư CUỐI CÙNG của tài khoản, không phải tổng phát
// sinh — proc gốc lấy nó từ dòng có RowNum lớn nhất (dòng 1367).
//
// Với tài khoản CHA, index được lưu vào closingSlot để patchParents() patch
// lại SoTon = tổng SoTon các con.
func (w *walker) closeAccount() {
	isParent := len(w.descendants[w.current]) > 0

	// Ghi lại SoTon cuối kỳ để tài khoản CHA cộng dồn sau. Dùng balance với
	// Debit/DebitQD mang chính SoTon/SoTonQD (Credit để 0) nên add() cộng đúng
	// cả số âm — SoTon là hiệu Nợ-Có nên có thể âm.
	w.closingSoTon[w.current] = balance{
		Debit:   w.running,
		DebitQD: w.runningQD,
	}

	idx := w.emit(Row{
		PositionOrder: PositionGroup,
		Account:       w.current,
		JournalMemo:   "Cộng nhóm",
		PhatSinhNo:    D(w.tc.Debit),
		PhatSinhCo:    D(w.tc.Credit),
		PhatSinhNoQD:  D(w.tc.DebitQD),
		PhatSinhCoQD:  D(w.tc.CreditQD),
		SoTon:         D(w.running),
		SoTonQD:       D(w.runningQD),
		ExchangeRate:  D(decimal.Zero),
	})
	if isParent {
		w.closingSlot[w.current] = idx
	}
}

// flushEmptyBefore phát trọn cặp (Tồn đầu kỳ + Cộng nhóm) cho mọi tài khoản
// đứng TRƯỚC target mà chưa được phát.
//
// Tài khoản không có phát sinh trong kỳ vẫn phải lên báo cáo với số tồn đầu kỳ
// của nó — proc dựng dòng tồn đầu kỳ cho TỪNG tài khoản trong danh sách (dòng
// 1042-1122), độc lập hoàn toàn với việc có bút toán hay không.
func (w *walker) flushEmptyBefore(target string) {
	for w.next < len(w.accounts) && w.accounts[w.next] != target {
		acc := w.accounts[w.next]
		w.next++
		if w.seen[acc] {
			continue
		}
		w.emitEmptyAccount(acc)
	}
	// Bỏ qua chính target: openAccount() ngay sau đó sẽ phát nó.
	if w.next < len(w.accounts) {
		w.next++
	}
}

// emitEmptyAccount phát một tài khoản chỉ có tồn đầu kỳ, không có phát sinh.
//
// Tài khoản CHA vẫn phát ngay như lá (reserve slot với SoTon tạm) — nhờ buffer
// (emit/flush), patchParents() patch lại đúng chỗ trước khi flush() gửi đi,
// nên vị trí hiển thị vẫn đúng thứ tự indexOf như OLTP.
func (w *walker) emitEmptyAccount(acc string) {
	if len(w.descendants[acc]) > 0 {
		w.pendingParents = append(w.pendingParents, acc)
	}
	w.openAccount(acc)
	w.closeAccount()
}

// finish đóng tài khoản đang mở, rồi phát nốt các tài khoản còn lại, patch
// SoTon các tài khoản cha, và cuối cùng gọi flush() gửi toàn bộ buffer đi.
func (w *walker) finish() error {
	if w.current != "" {
		w.closeAccount()
	}

	// Phần đuôi: tài khoản có số dư nhưng không phát sinh, nằm SAU tài khoản
	// cuối cùng có phát sinh. Các tài khoản xen giữa đã được flushEmptyBefore()
	// xử lý đúng vị trí rồi.
	for ; w.next < len(w.accounts); w.next++ {
		acc := w.accounts[w.next]
		if w.seen[acc] {
			continue
		}
		w.emitEmptyAccount(acc)
	}

	w.patchParents()
	return w.flush()
}

// patchParents sửa lại SoTon/SoTonQD của các dòng Tồn đầu kỳ + Cộng nhóm thuộc
// tài khoản CHA đã reserve ở openingSlot/closingSlot, NGAY TRONG buf — trước
// khi flush() gửi đi.
//
// Đúng cách Java hậu xử lý (DynamicReportQuyServiceImpl dòng 501-508): SoTon
// của cha = TỔNG SoTon các dòng Cộng nhóm CON. Kiểm chứng bằng số liệu thật:
//
//	1111:  593.433.786.727,69
//	1112:      561.706.518,65
//	1113:      -60.300.360,00
//	────────────────────────────
//	111:   593.935.192.886,34  ← khớp đúng web OLTP
//
// PhatSinhNo/Co giữ 0 — cha không có bút toán trực tiếp, proc cũng trả NULL
// cho hai cột này (đã kiểm chứng bằng EXEC proc trực tiếp).
//
// Xử lý theo thứ tự HẬU DUỆ ÍT NHẤT TRƯỚC để cây nhiều tầng patch đúng: cha
// tầng trên cần closingSoTon của cha tầng dưới, giá trị đó chỉ đúng SAU KHI
// cha tầng dưới đã được patch.
func (w *walker) patchParents() {
	if len(w.pendingParents) == 0 {
		return
	}

	pending := append([]string(nil), w.pendingParents...)
	sort.SliceStable(pending, func(i, j int) bool {
		return len(w.descendants[pending[i]]) < len(w.descendants[pending[j]])
	})

	for _, acc := range pending {
		var total balance
		for _, desc := range w.descendants[acc] {
			total.add(w.closingSoTon[desc])
		}
		// Cha có thể là hậu duệ của cha khác (cây nhiều tầng) — cập nhật để
		// tầng trên cộng đúng.
		w.closingSoTon[acc] = total

		// CHỈ patch dòng Cộng nhóm. Dòng Tồn đầu kỳ giữ nguyên giá trị đã tính
		// từ sddkMap (rollupOpeningBalances) — đó là TỒN ĐẦU KỲ thật của cha,
		// khác hẳn SoTon CUỐI KỲ ở đây. Patch cả hai là lỗi: đã kiểm chứng tồn
		// đầu kỳ của "111" (-1.071.319.319,97) khác hoàn toàn tổng SoTon cuối kỳ
		// các con (593.935.192.886,34).
		if idx, ok := w.closingSlot[acc]; ok {
			w.buf[idx].SoTon = D(total.Debit)
			w.buf[idx].SoTonQD = D(total.DebitQD)
		}
	}
}

// =============================================================================
// BUILD SQL
// =============================================================================

func buildDetailQuery(
	companyIDs, accounts []string, p QueryParams, noCol string, useStaging bool,
) string {
	sql := queryTemplate

	// ── HAI CẶP CỘT CỐ ĐỊNH, KHÔNG ĐỔI THEO typeShowCurrency ────────────────
	// Frontend render BỐN cột cùng lúc và bind cố định:
	//     "Nợ NT" / "Có NT"  ← PhatSinhNo   / PhatSinhCo    → NGUYÊN TỆ
	//     "Nợ"    / "Có"     ← PhatSinhNoQD / PhatSinhCoQD  → QUY ĐỔI
	//
	// Trước đây code cho PhatSinhNo chạy theo typeShowCurrency (0 → debit_amount)
	// nên cột "Nợ NT" nhận giá trị QUY ĐỔI. Lộ ra ở chứng từ ngoại tệ CTNB001:
	// fact ghi credit_amount = 13.186.500.000.000 (VND) và credit_amount_original
	// = 500.000.000 (USD), tỷ giá 26.373 — proc hiện 500 triệu ở "Có NT" còn DWH
	// hiện 13.186 tỷ ở "Nợ NT". Sai cả giá trị lẫn cột.
	//
	// Công ty chỉ dùng VND thì hai cặp bằng nhau nên bug không lộ.
	debitShow, creditShow := "f.debit_amount_original", "f.credit_amount_original"

	if p.GroupTheSameItem == 1 {
		// ── CHẾ ĐỘ GỘP ──────────────────────────────────────────────────────
		//
		// ── VÌ SAO KHÔNG DỊCH THẲNG GROUP BY 4 CỘT CỦA PROC ─────────────────
		// Proc chạy HAI BƯỚC trên hai bảng có grain KHÁC NHAU:
		//
		//   B1 (dòng 350-370): gộp trên GL ⟕ GLD, join bằng CẶP khoá
		//        on gl.DetailID = GLD.DetailID and gl.ReferenceID = GLD.ReferenceID
		//      Vế trái GL đã ở grain DÒNG nên join 1-1 → SUM chỉ cộng đúng
		//      phần thuộc nhóm.
		//        group by GL.ReferenceID, Account, AccountCorresponding, AccountingObjectID
		//
		//   B2 (dòng 550-602): UPDATE ... FROM GeneralLedger GL
		//                      WHERE ReferenceIDTem = GL.ReferenceID
		//      Lấy Date/NoFBook/Reason/ExchangeRate ở MỨC HEADER.
		//
		// fact_gl_entry_line đã phẳng sẵn ở grain GLD (một dòng = một GLD).
		// Dịch nguyên GROUP BY 4 cột lên đó thì SUM cộng MỌI dòng GLD cùng
		// reference_id + cùng cặp tài khoản → chứng từ 2 dòng GLD ra 2×, chứng
		// từ 1 dòng ra 1×. Đã gặp thật: PC02/PC03/PC05/PC25/PC27 gấp đôi trong
		// khi HMTLY001 và PC26 đúng.
		//
		// Cách sửa: đưa các cột LẤY TỪ HEADER ở B2 vào GROUP BY. Chúng hằng số
		// trong mỗi reference_id (B2 khớp chỉ theo ReferenceID) nên KHÔNG làm
		// nhóm vỡ thêm, nhưng chặn được việc gộp nhầm các dòng GLD khác nhau.
		// Cùng cách Proc_SO_CHI_TIET_CAC_TAI_KHOAN đã sửa — xem ghi chú
		// "Trước đây dùng any() ... là nguyên nhân ClickHouse gộp nhiều hơn OLTP".
		sql = strings.ReplaceAll(sql, "{{REF_ID_EXPR}}", "f.reference_id")

		// Các cột dưới đây NẰM TRONG GROUP BY nên tham chiếu trực tiếp.
		sql = strings.ReplaceAll(sql, "{{TYPE_ID_EXPR}}", "f.type_id")
		sql = strings.ReplaceAll(sql, "{{DATE_EXPR}}", "f.voucher_date")
		sql = strings.ReplaceAll(sql, "{{POSTED_DATE_EXPR}}", "f.posted_date")
		sql = strings.ReplaceAll(sql, "{{NO_EXPR}}", "f."+noCol)
		sql = strings.ReplaceAll(sql, "{{REASON_EXPR}}", "f.reason")
		sql = strings.ReplaceAll(sql, "{{JOURNAL_MEMO_EXPR}}", "f.reason")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_EXPR}}", "f.account_number")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_CORRESPONDING_EXPR}}", "f.account_corresponding")

		sql = strings.ReplaceAll(sql, "{{DEBIT_EXPR}}", "sum(coalesce("+debitShow+", 0))")
		sql = strings.ReplaceAll(sql, "{{CREDIT_EXPR}}", "sum(coalesce("+creditShow+", 0))")
		sql = strings.ReplaceAll(sql, "{{DEBIT_QD_EXPR}}", "sum(coalesce(f.debit_amount, 0))")
		sql = strings.ReplaceAll(sql, "{{CREDIT_QD_EXPR}}", "sum(coalesce(f.credit_amount, 0))")

		// exchange_rate nằm trong GROUP BY nhưng vẫn Nullable — coalesce để khớp
		// kiểu với chế độ chi tiết (scanRow đọc vào giá trị, không phải con trỏ).
		sql = strings.ReplaceAll(sql, "{{EXCHANGE_RATE_EXPR}}", "coalesce(f.exchange_rate, 0)")

		// order_priority là cột MỨC DÒNG, proc KHÔNG gộp theo nó → any().
		sql = strings.ReplaceAll(sql, "{{ORDER_PRIORITY_EXPR}}", "any(f.order_priority)")
		// line_bk cũng mức dòng — chỉ dùng để tie-break ORDER BY, không ảnh
		// hưởng logic gộp. Xem ghi chú {{LINE_BK_EXPR}} ở chế độ chi tiết.
		sql = strings.ReplaceAll(sql, "{{LINE_BK_EXPR}}", "any(f.line_bk)")

		sql = strings.ReplaceAll(sql, "{{AO_CODE_EXPR}}", "f.accounting_object_code")
		sql = strings.ReplaceAll(sql, "{{AO_NAME_EXPR}}", "f.accounting_object_name")

		for i := 1; i <= 5; i++ {
			// custom_field lấy từ header ở B2 → vào GROUP BY.
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CF%d}}", i),
				fmt.Sprintf("f.custom_field%d", i))
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CFD%d}}", i),
				fmt.Sprintf("f.custom_field_detail%d", i))
		}

		// Proc gộp theo AccountingObjectID; fact chỉ có code/name nên dùng code
		// làm khoá thay thế. Hai đối tượng trùng mã sẽ bị gộp — hiếm, nhưng
		// đáng biết.
		sql = strings.ReplaceAll(sql, "{{GROUP_BY_CLAUSE}}",
			"GROUP BY f.reference_id, f.account_number, f.account_corresponding, "+
				"f.accounting_object_code, f.accounting_object_name, "+
				"f.type_id, f.voucher_date, f.posted_date, f."+noCol+", "+
				"f.reason, f.exchange_rate, "+
				"f.custom_field1, f.custom_field2, f.custom_field3, "+
				"f.custom_field4, f.custom_field5, "+
				"f.custom_field_detail1, f.custom_field_detail2, "+
				"f.custom_field_detail3, f.custom_field_detail4, f.custom_field_detail5")

	} else {
		// ── CHẾ ĐỘ CHI TIẾT ─────────────────────────────────────────────────
		sql = strings.ReplaceAll(sql, "{{REF_ID_EXPR}}", "f.reference_id")
		sql = strings.ReplaceAll(sql, "{{TYPE_ID_EXPR}}", "f.type_id")
		// voucher_date = GL.Date (ngày chứng từ), posted_date = ngày hạch toán.
		// HAI CỘT KHÁC NHAU, KHÔNG ĐƯỢC DÙNG LẪN: voucher_date nằm trong ORDER BY
		// (giữa posted_date và ca_type) nên lấy sai cột là đổi THỨ TỰ DÒNG, kéo
		// theo SoTon luỹ kế của từng dòng sai — dù tổng cuối kỳ vẫn đúng.
		sql = strings.ReplaceAll(sql, "{{DATE_EXPR}}", "f.voucher_date")
		sql = strings.ReplaceAll(sql, "{{POSTED_DATE_EXPR}}", "f.posted_date")
		sql = strings.ReplaceAll(sql, "{{NO_EXPR}}", "f."+noCol)
		sql = strings.ReplaceAll(sql, "{{REASON_EXPR}}", "f.reason")
		sql = strings.ReplaceAll(sql, "{{JOURNAL_MEMO_EXPR}}", "f.reason")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_EXPR}}", "f.account_number")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_CORRESPONDING_EXPR}}", "f.account_corresponding")

		sql = strings.ReplaceAll(sql, "{{DEBIT_EXPR}}", "coalesce("+debitShow+", 0)")
		sql = strings.ReplaceAll(sql, "{{CREDIT_EXPR}}", "coalesce("+creditShow+", 0)")
		sql = strings.ReplaceAll(sql, "{{DEBIT_QD_EXPR}}", "coalesce(f.debit_amount, 0)")
		sql = strings.ReplaceAll(sql, "{{CREDIT_QD_EXPR}}", "coalesce(f.credit_amount, 0)")

		sql = strings.ReplaceAll(sql, "{{EXCHANGE_RATE_EXPR}}", "coalesce(f.exchange_rate, 0)")
		sql = strings.ReplaceAll(sql, "{{ORDER_PRIORITY_EXPR}}", "f.order_priority")
		// line_bk = ID gốc của GeneralLedgerDetail, tăng dần theo thứ tự ghi sổ
		// vật lý. Dùng làm tie-break CUỐI của ORDER BY khi mọi cột khác giống
		// hệt nhau (cùng chứng từ, cùng ngày, cùng OrderPriority) — ví dụ NVK060
		// có 2 dòng 1111→1111/1111→1331 hoàn toàn giống nhau ở các cột còn lại.
		// Không có nó, ClickHouse và SQL Server có thể trả thứ tự khác nhau,
		// đảo lộn SoTon lũy kế TỪNG DÒNG (tổng cuối vẫn đúng, nhưng số ở giữa
		// sai) — đã gặp thật khi đối chiếu NVK060.
		sql = strings.ReplaceAll(sql, "{{LINE_BK_EXPR}}", "f.line_bk")
		sql = strings.ReplaceAll(sql, "{{AO_CODE_EXPR}}", "f.accounting_object_code")
		sql = strings.ReplaceAll(sql, "{{AO_NAME_EXPR}}", "f.accounting_object_name")

		for i := 1; i <= 5; i++ {
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CF%d}}", i),
				fmt.Sprintf("f.custom_field%d", i))
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CFD%d}}", i),
				fmt.Sprintf("f.custom_field_detail%d", i))
		}

		sql = strings.ReplaceAll(sql, "{{GROUP_BY_CLAUSE}}", "")
	}

	return applyCommonPlaceholders(sql, companyIDs, accounts, p, useStaging)
}

func buildOpeningQuery(companyIDs, accounts []string, p QueryParams, useStaging bool) string {
	return applyCommonPlaceholders(openingTemplate, companyIDs, accounts, p, useStaging)
}

// ── PHƯƠNG PHÁP XÁC ĐỊNH SỐ DƯ ĐẦU KỲ ───────────────────────────────────────
// Frontend có tuỳ chọn openingBalanceMethod với hai lựa chọn:
//
//	[1] "Theo số liệu tổng hợp từ chứng từ" — MẶC ĐỊNH, đang dùng.
//	    Cộng dồn toàn bộ chứng từ trước kỳ báo cáo → query_opening.sql.
//
//	[2] "Theo kết chuyển số dư cuối năm trước" — CHƯA triển khai.
//	    Dùng số dư đã kết chuyển. OLTP có proc riêng cho nhánh này:
//	    Proc_SO_QUY_TIEN_MAT_THEO_KET_CHUYEN_SO_DU_CUOI_NAM.
//	    Nguồn DWH tương ứng: eb_dwh.fact_account_opening_period.
//
// Hai lựa chọn KHÔNG phải cái đúng cái sai — đổi nguồn của [1] sang bảng chốt
// kỳ là làm sai phương pháp mặc định.
//
// Khi triển khai [2]: thêm OpeningBalanceMethod vào QueryParams, đọc từ
// request, rồi rẽ nhánh trong loadOpeningBalances(). Lưu ý bảng chốt kỳ hiện
// chỉ có 1.468 dòng / 16 công ty và nhiều công ty rỗng, nên phải kiểm ETL đã
// nạp đủ trước khi bật.

func applyCommonPlaceholders(
	sql string, companyIDs, accounts []string, p QueryParams, useStaging bool,
) string {
	accList := quoteJoin(accounts)

	glSource := factSourceExpr
	if useStaging {
		glSource = stagingSourceExpr
	}
	sql = strings.ReplaceAll(sql, "{{GL_SOURCE}}", glSource)

	sql = strings.ReplaceAll(sql, "{{COMPANY_IDS}}", quoteJoin(companyIDs))
	sql = strings.ReplaceAll(sql, "{{FROM_DATE}}", p.FromDate)
	sql = strings.ReplaceAll(sql, "{{TO_DATE}}", p.ToDate)
	sql = strings.ReplaceAll(sql, "{{TYPE_LEDGER}}", fmt.Sprintf("%d", p.TypeLedger))
	sql = strings.ReplaceAll(sql, "{{ACCOUNT_NUMBERS}}", accList)
	sql = strings.ReplaceAll(sql, "{{ACCOUNT_ORDER}}", accList)
	sql = strings.ReplaceAll(sql, "{{CURRENCY_FILTER}}", currencyFilter(p.CurrencyID))
	sql = strings.ReplaceAll(sql, "{{CLUSTER_FILTER}}", clusterFilter(p.ClusterID))
	// query_opening.sql không có placeholder này; ReplaceAll bỏ qua nếu không
	// tìm thấy nên gọi ở đây là an toàn.
	sql = strings.ReplaceAll(sql, "{{GROUP_BY_CLAUSE}}", "")
	return sql
}

// =============================================================================
// TRUY VẤN PHỤ TRỢ
// =============================================================================

func (s *Service) resolveCompanyScope(ctx context.Context, p QueryParams) ([]string, error) {
	if p.IsDependent != 1 {
		return p.CompanyIDs, nil
	}
	sql := fmt.Sprintf(`
		SELECT DISTINCT toString(child_org_id)
		FROM eb_dwh.bridge_org_scope
		WHERE root_org_id = '%s' AND child_org_id != root_org_id`, p.PrimaryCompanyID)

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := append([]string{}, p.CompanyIDs...)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return dedupe(ids), rows.Err()
}

// expandAccounts BO SUNG tai khoan con vao danh sach, GIU NGUYEN tai khoan cha.
//
// GIU CHA LA BAT BUOC — dung khong phai "mo rong thanh danh sach la".
// Ban Java cu (DynamicReportQuyServiceImpl dong 366-371) lam dung the:
//
//	accountListService.getListChildAccount(accountListsTemp, accountList.get().getId());
//	accountLists.add(accountList.get());       // <-- THEM CHINH TAI KHOAN CHA
//	accountLists.addAll(accountListsTemp);     // <-- roi moi them cac con
//
// Nguoi dung chon "111" thi bao cao hien MOT dong "111" (tong ca nhanh) VA cac
// dong "1111", "1112", "1113", "1114". Bo cha di la mat han dong tong — dung
// loi da gap: DWH khong co dong 111 nao trong khi proc co.
//
// Vi danh sach chua CA cha lan con, tinh ton dau ky cua cha PHAI cong don theo
// cay (xem rollupOpeningBalances) — khong the khop chinh xac ma.
//
// Bridge chua build xong thi dung chinh danh sach goc — giu dung hanh vi ban
// Java cu, tha hien thieu tai khoan con con hon tra ve rong.
func (s *Service) expandAccounts(
	ctx context.Context, companyID string, parents []string,
) ([]string, error) {
	sql := fmt.Sprintf(`
		SELECT DISTINCT b.descendant_account_number
		FROM eb_dwh.bridge_account_hierarchy b
		INNER JOIN eb_dwh.dim_account d
		    ON  d.company_id     = b.company_id
		    AND d.account_number = b.descendant_account_number
		    AND d.is_current     = 1
		    AND d.is_parent_node = 0
		WHERE b.company_id = '%s'
		  AND b.ancestor_account_number IN (%s)`, companyID, quoteJoin(parents))

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Cha dung TRUOC theo dung thu tu nguoi dung chon — {{ACCOUNT_ORDER}} dung
	// indexOf() tren danh sach nay de sap xep bao cao.
	out := append([]string{}, parents...)
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// cleanAccounts da dedupe, nen tai khoan la vua la cha vua la con (bridge
	// thuong co dong self-reference depth=0) khong bi lap.
	return cleanAccounts(out), nil
}

// filterAccountsByCurrency loc danh sach tai khoan theo LOAI TIEN cua chinh
// TAI KHOAN — tuong duong @tblListAccountByCurrency cua proc (dong 284-336).
//
// ── KHAC VOI currencyFilter() ───────────────────────────────────────────────
// currencyFilter() loc theo currency_code cua GIAO DICH. Ham nay loc theo
// is_foreign_currency cua TAI KHOAN. Proc ap CA HAI (dong 365-366):
//
//	AND GLD.Account IN (SELECT Account from @tblListAccount)
//	AND GLD.Account IN (SELECT Account from @tblListAccountByCurrency)
//
// Thieu ham nay thi tai khoan ngoai te co giao dich VND van len bao cao khi
// nguoi dung chon VND — proc thi chan.
//
// ── QUY TAC ─────────────────────────────────────────────────────────────────
// currencyID = dong tien hach toan  → chi tai khoan is_foreign_currency = 0
// currencyID != dong tien hach toan → chi tai khoan is_foreign_currency = 1
// currencyID = 'TH' hoac rong       → LAY TAT CA, khong loc
//
// Tai khoan '111' LUON duoc giu bat ke loai tien (proc dong 336) vi no la node
// tong hop cua ca hai nhanh noi/ngoai te.
//
// Bang dim_account loi hoac chua co du lieu thi tra ve danh sach goc — tha
// hien thua tai khoan con hon lam bao cao trong.
func (s *Service) filterAccountsByCurrency(
	ctx context.Context, companyID string, accounts []string, p QueryParams,
) ([]string, error) {
	// 'TH' = tong hop tat ca loai tien → khong loc.
	if p.CurrencyID == "" || p.CurrencyID == "TH" {
		return accounts, nil
	}

	mainCurrency, err := s.mainCurrency(ctx, companyID)
	if err != nil || mainCurrency == "" {
		return accounts, nil
	}

	wantForeign := 0
	if p.CurrencyID != mainCurrency {
		wantForeign = 1
	}

	sql := fmt.Sprintf(`
		SELECT DISTINCT d.account_number
		FROM eb_dwh.dim_account d
		WHERE d.company_id           = '%s'
		  AND d.is_current           = 1
		  AND d.is_foreign_currency  = %d
		  AND d.account_number IN (%s)`,
		companyID, wantForeign, quoteJoin(accounts))

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return accounts, nil
	}
	defer rows.Close()

	keep := make(map[string]bool, len(accounts))
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		keep[a] = true
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(keep) == 0 {
		return accounts, nil
	}

	// Giu thu tu ban dau — {{ACCOUNT_ORDER}} dung indexOf() tren danh sach nay.
	out := make([]string, 0, len(accounts))
	for _, a := range accounts {
		if keep[a] || a == "111" {
			out = append(out, a)
		}
	}
	if len(out) == 0 {
		return accounts, nil
	}
	return out, nil
}

// mainCurrency tra ve dong tien hach toan.
//
// Uu tien dim_currency.is_base_currency; khong co thi suy tu fact.
//
// ── LUU Y: dim_currency KHONG CO company_id ─────────────────────────────────
// Day la dim TOAN CUC — mot dong tien goc cho ca he thong. Proc thi lay theo
// TUNG cong ty (EbOrganizationUnit.CurrencyID, dong 280). Hai cach chi lech
// khi cac cong ty trong he thong dung dong tien hach toan KHAC nhau; luc do
// phai bo sung company_id vao dim (hoac cot currency vao dim_org_unit) va sua
// ham nay.
func (s *Service) mainCurrency(ctx context.Context, companyID string) (string, error) {
	if c, err := s.baseCurrencyFromDim(ctx); err == nil && c != "" {
		return c, nil
	}
	return s.mainCurrencyFromFact(ctx, companyID)
}

// baseCurrencyFromDim doc dong tien goc tu dim_currency.
func (s *Service) baseCurrencyFromDim(ctx context.Context) (string, error) {
	sql := `
		SELECT currency_code
		FROM eb_dwh.dim_currency
		WHERE is_base_currency = 1 AND is_active = 1
		LIMIT 1`

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", err
		}
		return c, rows.Err()
	}
	return "", rows.Err()
}

// mainCurrencyFromFact suy dong tien hach toan TU CHINH SO LIEU — phuong an du
// phong khi dim_currency chua co du lieu.
//
// But toan ghi bang DONG TIEN HACH TOAN co so quy doi BANG so nguyen te (khong
// quy doi gi); but toan ngoai te thi hai cot khac nhau. Nen dong tien xuat hien
// nhieu nhat trong nhom "hai cot bang nhau" chinh la dong tien hach toan.
//
// Loc amount != 0 de bo dong rong (0 = 0 dung voi moi loai tien).
//
// Khong suy duoc (cong ty moi, chua co but toan) thi tra ve rong →
// filterAccountsByCurrency() bo qua buoc loc, giu hanh vi cu.
func (s *Service) mainCurrencyFromFact(ctx context.Context, companyID string) (string, error) {
	sql := fmt.Sprintf(`
		SELECT coalesce(f.currency_code, '') AS ccy
		FROM eb_dwh.fact_gl_entry_line AS f FINAL
		WHERE f.company_id = '%s'
		  AND f.is_deleted = 0
		  AND coalesce(f.currency_code, '') != ''
		  AND (
		        (coalesce(f.debit_amount, 0)  != 0
		     AND  f.debit_amount  = f.debit_amount_original)
		     OR (coalesce(f.credit_amount, 0) != 0
		     AND  f.credit_amount = f.credit_amount_original)
		      )
		GROUP BY ccy
		ORDER BY count() DESC
		LIMIT 1`, companyID)

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return "", err
		}
		return c, rows.Err()
	}
	return "", rows.Err()
}

// loadOpeningBalances — cột nguyên tệ vào Debit/Credit, quy đổi vào
// DebitQD/CreditQD. KHÔNG đổi theo typeShowCurrency: xem ghi chú ở
// query_opening.sql.
func (s *Service) loadOpeningBalances(
	ctx context.Context, companyIDs, accounts []string, p QueryParams,
) (map[string]balance, error) {
	rows, err := s.conn.Query(ctx, buildOpeningQuery(companyIDs, accounts, p, s.useStaging))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]balance)
	for rows.Next() {
		var acc string
		var dOC, cOC, dQD, cQD decimal.Decimal
		if err := rows.Scan(&acc, &dOC, &cOC, &dQD, &cQD); err != nil {
			return nil, err
		}
		out[acc] = balance{Debit: dOC, Credit: cOC, DebitQD: dQD, CreditQD: cQD}
	}
	return out, rows.Err()
}

// rollupOpeningBalances cong don ton dau ky theo CAY tai khoan.
//
// ── VI SAO CAN, DU TRUOC DAY GHI CHU NOI LA KHONG ───────────────────────────
// Proc goc (dong 1045-1071) dung tung tai khoan mot cay de quy roi SUM ca
// nhanh. Nhan dinh cu — "danh sach da phang nen orderedTree(la) la no-op" —
// chi dung voi tai khoan LA. Voi tai khoan CHA nhu "111" thi orderedTree tra
// ve ca nhanh con, va SUM la cong don that.
//
// Chung cu tu du lieu that: proc cho 111 ton dau ky NT = -1,071,319,319.97
// trong khi 1111 = -972,512,982.62. Hai so khac nhau, tuc 111 KHONG chi lay
// but toan hach toan truc tiep vao no ma cong ca 1111 + 1112 + 1113 + 1114.
//
// Khong the thay bang "IN (danh sach hau due)" trong SQL vi query_opening.sql
// GROUP BY account_number — moi tai khoan mot dong rieng. Cong don phai lam o
// day, sau khi da co so lieu tung ma.
//
// Luu y: KHONG cong don tich luy chong len nhau. Moi tai khoan lay tong cua
// chinh no + toan bo hau due, doc tu ban goc `direct`, nen cay nhieu tang cung
// khong bi dem trung.
func (s *Service) rollupOpeningBalances(
	ctx context.Context, companyID string, accounts []string,
	direct map[string]balance,
) (map[string]balance, map[string][]string, error) {
	descendants, err := s.loadDescendants(ctx, companyID, accounts)
	if err != nil {
		return nil, nil, err
	}
	if len(descendants) == 0 {
		return direct, descendants, nil
	}

	out := make(map[string]balance, len(accounts))
	for _, acc := range accounts {
		total := direct[acc] // zero value neu tai khoan khong co phat sinh

		for _, desc := range descendants[acc] {
			total.add(direct[desc])
		}
		out[acc] = total
	}
	return out, descendants, nil
}

// loadDescendants tra ve map tai khoan -> danh sach HAU DUE (khong ke chinh no)
// trong pham vi danh sach `accounts` da duoc mo rong.
//
// Dung cho CA HAI viec cong don theo cay:
//   - ton dau ky (rollupOpeningBalances)
//   - SoTon dong Cong nhom cua tai khoan cha (walker.pendingParents)
//
// Bridge loi thi tra ve map rong — goi ben ngoai tu quyet dinh thoai hoa an
// toan (khop chinh xac ma, thieu phan cong don) chu khong lam bao cao chet.
func (s *Service) loadDescendants(
	ctx context.Context, companyID string, accounts []string,
) (map[string][]string, error) {
	sql := fmt.Sprintf(`
		SELECT b.ancestor_account_number, b.descendant_account_number
		FROM eb_dwh.bridge_account_hierarchy b
		WHERE b.company_id = '%s'
		  AND b.ancestor_account_number IN (%s)`, companyID, quoteJoin(accounts))

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return map[string][]string{}, nil
	}
	defer rows.Close()

	// Chi giu hau due NAM TRONG danh sach dang bao cao: bridge co the chua ca
	// nhanh khong duoc chon, cong vao la sai.
	inScope := make(map[string]bool, len(accounts))
	for _, a := range accounts {
		inScope[a] = true
	}

	out := make(map[string][]string, len(accounts))
	for rows.Next() {
		var ancestor, descendant string
		if err := rows.Scan(&ancestor, &descendant); err != nil {
			return nil, err
		}
		// Bridge thuong co dong self-reference (depth = 0); giu lai la dem doi
		// chinh tai khoan cha.
		if descendant == ancestor || !inScope[descendant] {
			continue
		}
		out[ancestor] = append(out[ancestor], descendant)
	}
	return out, rows.Err()
}

// =============================================================================
// SCAN
// =============================================================================

// rawRow là dòng đọc từ SQL — đã tách sẵn thu/chi bằng UNION ALL.
type rawRow struct {
	RefID                string
	TypeID               int32
	VoucherDate          time.Time
	PostedDate           time.Time
	ReceiptRefNo         string
	PaymentRefNo         string
	CAType               int32
	Reason               string
	JournalMemo          string
	Account              string
	AccountCorresponding string

	PhatSinhNo   decimal.Decimal
	PhatSinhCo   decimal.Decimal
	PhatSinhNoQD decimal.Decimal
	PhatSinhCoQD decimal.Decimal

	ExchangeRate  decimal.Decimal
	OrderPriority int32
	LineBK        string // tie-break ORDER BY — xem ghi chú ở query.sql

	AccountingObjectCode string
	AccountingObjectName string
	CustomField          [5]string
	CustomFieldDetail    [5]string
}

// scanRow đọc một dòng kết quả.
//
// QUY TẮC: cột Nullable BẮT BUỘC scan vào con trỏ, cột không Nullable BẮT BUỘC
// scan vào giá trị. Sai chiều nào cũng lỗi lúc chạy.
//
// Các cột tiền đã coalesce trong SQL nên không bao giờ NULL → scan vào giá trị.
// receipt_ref_no / payment_ref_no dùng ” thay NULL trong UNION ALL nên cũng là
// giá trị.
func scanRow(rows driverRows) (rawRow, error) {
	var (
		refID  *uuid.UUID
		typeID *int32
		// voucher_date là Nullable(Date) trong fact → PHẢI scan vào con trỏ.
		// posted_date không Nullable → scan vào giá trị. Xem quy tắc ở doc comment.
		voucherDate          *time.Time
		postedDate           time.Time
		receiptRefNo         string
		paymentRefNo         string
		caType               int32
		reason               *string
		journalMemo          *string
		account              string
		accountCorresponding *string

		phatSinhNo   decimal.Decimal
		phatSinhCo   decimal.Decimal
		phatSinhNoQD decimal.Decimal
		phatSinhCoQD decimal.Decimal
		exchangeRate decimal.Decimal

		orderPriority  *int32
		lineBK         string // không Nullable — any(f.line_bk)/f.line_bk/toString(gld.ID)
		aoCode, aoName *string
		cf             [5]*string
		cfd            [5]*string
	)

	if err := rows.Scan(
		&refID, &typeID, &voucherDate, &postedDate,
		&receiptRefNo, &paymentRefNo, &caType,
		&reason, &journalMemo, &account, &accountCorresponding,
		&phatSinhNo, &phatSinhCo, &phatSinhNoQD, &phatSinhCoQD,
		&exchangeRate, &orderPriority, &lineBK,
		&aoCode, &aoName,
		&cf[0], &cf[1], &cf[2], &cf[3], &cf[4],
		&cfd[0], &cfd[1], &cfd[2], &cfd[3], &cfd[4],
	); err != nil {
		return rawRow{}, err
	}

	r := rawRow{
		RefID:                uuidOrEmpty(refID),
		TypeID:               int32OrZero(typeID),
		VoucherDate:          timeOrZero(voucherDate),
		PostedDate:           postedDate,
		ReceiptRefNo:         receiptRefNo,
		PaymentRefNo:         paymentRefNo,
		CAType:               caType,
		Reason:               strOrEmpty(reason),
		JournalMemo:          strOrEmpty(journalMemo),
		Account:              account,
		AccountCorresponding: strOrEmpty(accountCorresponding),
		PhatSinhNo:           phatSinhNo,
		PhatSinhCo:           phatSinhCo,
		PhatSinhNoQD:         phatSinhNoQD,
		PhatSinhCoQD:         phatSinhCoQD,
		ExchangeRate:         exchangeRate,
		OrderPriority:        int32OrZero(orderPriority),
		LineBK:               lineBK,
		AccountingObjectCode: strOrEmpty(aoCode),
		AccountingObjectName: strOrEmpty(aoName),
	}
	for i := 0; i < 5; i++ {
		r.CustomField[i] = strOrEmpty(cf[i])
		r.CustomFieldDetail[i] = strOrEmpty(cfd[i])
	}
	return r, nil
}

// =============================================================================
// VALIDATE + HELPERS
// =============================================================================

func validateParams(p QueryParams) error {
	if len(p.CompanyIDs) == 0 {
		return fmt.Errorf("companyIds khong duoc rong")
	}
	if p.PrimaryCompanyID == "" {
		return fmt.Errorf("primaryCompanyId khong duoc rong")
	}
	if len(p.AccountNumbers) == 0 {
		return fmt.Errorf("accountNumbers khong duoc rong")
	}

	ids := append([]string{}, p.CompanyIDs...)
	ids = append(ids, p.PrimaryCompanyID)
	for _, id := range ids {
		if !uuidPattern.MatchString(id) {
			return fmt.Errorf("ID khong dung dinh dang UUID: %s", id)
		}
	}

	// Ngày PHẢI parse được, không chỉ kiểm rỗng — chúng đi thẳng vào chuỗi SQL.
	from, err := time.Parse("2006-01-02", p.FromDate)
	if err != nil {
		return fmt.Errorf("fromDate phai dang YYYY-MM-DD: %q", p.FromDate)
	}
	to, err := time.Parse("2006-01-02", p.ToDate)
	if err != nil {
		return fmt.Errorf("toDate phai dang YYYY-MM-DD: %q", p.ToDate)
	}
	if to.Before(from) {
		return fmt.Errorf("toDate som hon fromDate")
	}

	if p.CurrencyID != "" && p.CurrencyID != "TH" && !currencyPattern.MatchString(p.CurrencyID) {
		return fmt.Errorf("currencyId khong hop le: %q", p.CurrencyID)
	}
	if p.ClusterID != "" && !clusterPattern.MatchString(p.ClusterID) {
		return fmt.Errorf("clusterId khong hop le: %q", p.ClusterID)
	}
	if p.TypeLedger != 0 && p.TypeLedger != 1 {
		return fmt.Errorf("typeLedger phai la 0 (tai chinh) hoac 1 (quan tri)")
	}
	if p.TypeShowCurrency != 0 && p.TypeShowCurrency != 1 {
		return fmt.Errorf("typeShowCurrency phai la 0 (quy doi) hoac 1 (nguyen te)")
	}
	if p.GroupTheSameItem != 0 && p.GroupTheSameItem != 1 {
		return fmt.Errorf("groupTheSameItem phai la 0 hoac 1")
	}
	return nil
}

func currencyFilter(currencyID string) string {
	if currencyID == "" || currencyID == "TH" {
		return ""
	}
	return fmt.Sprintf("AND f.currency_code = '%s'", currencyID)
}

func clusterFilter(clusterID string) string {
	if clusterID == "" {
		return ""
	}
	return fmt.Sprintf("AND f.cluster_id = '%s'", clusterID)
}

func cleanAccounts(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, a := range in {
		a = strings.TrimSpace(a)
		if a == "" || a == "Tất cả" || seen[a] {
			continue
		}
		seen[a] = true
		out = append(out, a)
	}
	return out
}

func dedupe(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func quoteJoin(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	q := make([]string, len(ids))
	for i, id := range ids {
		q[i] = fmt.Sprintf("'%s'", id)
	}
	return strings.Join(q, ", ")
}

func uuidOrEmpty(u *uuid.UUID) string {
	if u == nil {
		return ""
	}
	return u.String()
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func int32OrZero(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}

// timeOrZero trả về zero time khi NULL. formatDate() ở grpc-server.go dịch zero
// time thành chuỗi rỗng, khớp cách proc gốc trả NULL cho cột Date.
func timeOrZero(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

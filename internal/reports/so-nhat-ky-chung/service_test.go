package so_nhat_ky_chung

import (
	"strings"
	"testing"
)

// Mọi tổ hợp (mẫu mặc định × gộp × staging) phải điền hết placeholder: sót một cái
// là câu SQL sai cú pháp chỉ lộ ra khi chạy trên ClickHouse.
func TestBuildQueryFillsAllPlaceholders(t *testing.T) {
	ids := []string{"3d54febe-0c40-4a4f-a2b3-dd5f4a82e428"}
	for _, isDefault := range []bool{false, true} {
		for _, group := range []int{0, 1} {
			for _, staging := range []bool{false, true} {
				p := QueryParams{
					CompanyIDs: ids, PrimaryCompanyID: ids[0],
					FromDate: "2025-01-01", ToDate: "2025-01-31",
					IsFinancialBook: 1, GroupSameItem: group, IsDefaultType: isDefault,
					ClusterID: "EB99_GR2",
				}
				for name, sql := range map[string]string{
					"detail":  buildDetailQuery(ids, p, 0, "no_fbook", staging),
					"count":   buildCountQuery(ids, p, 0, "no_fbook", staging),
					"opening": buildOpeningQuery(ids, p, 0, staging),
				} {
					if strings.Contains(sql, "{{") {
						t.Errorf("%s (default=%v group=%d staging=%v) còn placeholder:\n%s",
							name, isDefault, group, staging, sql)
					}
				}
			}
		}
	}
}

// Câu đếm bọc đúng truy vấn chi tiết nhưng không được mang ORDER BY / SETTINGS
// bên trong truy vấn con.
func TestCountQueryHasNoOrderBy(t *testing.T) {
	ids := []string{"3d54febe-0c40-4a4f-a2b3-dd5f4a82e428"}
	p := QueryParams{CompanyIDs: ids, PrimaryCompanyID: ids[0], FromDate: "2025-01-01", ToDate: "2025-01-31"}
	sql := buildCountQuery(ids, p, 1, "no_mbook", false)
	if strings.Contains(sql, "ORDER BY") {
		t.Errorf("câu đếm không được có ORDER BY:\n%s", sql)
	}
	if !strings.HasPrefix(sql, "SELECT count() FROM (") {
		t.Errorf("câu đếm phải bọc truy vấn chi tiết:\n%s", sql)
	}
}

func TestValidateParams(t *testing.T) {
	ok := QueryParams{
		CompanyIDs: []string{"3d54febe-0c40-4a4f-a2b3-dd5f4a82e428"}, PrimaryCompanyID: "3d54febe-0c40-4a4f-a2b3-dd5f4a82e428",
		FromDate: "2025-01-01", ToDate: "2025-01-31",
	}
	if err := validateParams(ok); err != nil {
		t.Fatalf("tham số hợp lệ bị từ chối: %v", err)
	}
	bad := ok
	bad.ToDate = "2025-01-31' OR 1=1 --"
	if validateParams(bad) == nil {
		t.Error("toDate chèn SQL phải bị từ chối")
	}
	bad = ok
	bad.ClusterID = "x'; DROP"
	if validateParams(bad) == nil {
		t.Error("clusterId chèn SQL phải bị từ chối")
	}
	bad = ok
	bad.GroupSameItem = 2
	if validateParams(bad) == nil {
		t.Error("groupSameItem ngoài 0/1 phải bị từ chối")
	}
}

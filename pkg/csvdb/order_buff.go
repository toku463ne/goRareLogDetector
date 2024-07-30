package csvdb

import "strconv"

type orderBuffRow struct {
	v               []string
	orderFieldTypes []string
	orderFieldIdxs  []int
	direction       int
}

type orderBuffRows []orderBuffRow

func (ov orderBuffRows) Len() int {
	return len(ov)
}
func (ov orderBuffRows) Swap(i, j int) {
	ov[i], ov[j] = ov[j], ov[i]
}
func (ov orderBuffRows) Less(i, j int) bool {
	for k, fieldt := range ov[i].orderFieldTypes {
		idx := ov[i].orderFieldIdxs[k]
		switch fieldt {
		case "int", "int8", "int32", "int64", "bool":
			d := int64(ov[i].direction)
			r1, _ := strconv.ParseInt(ov[i].v[idx], 10, 64)
			r2, _ := strconv.ParseInt(ov[j].v[idx], 10, 64)
			if r1*d < r2*d {
				return true
			} else if r1*d > r2*d {
				return false
			}
		case "uint", "uint8", "uint16", "uint32", "uint64":
			d := uint64(ov[i].direction)
			r1, _ := strconv.ParseUint(ov[i].v[idx], 10, 64)
			r2, _ := strconv.ParseUint(ov[j].v[idx], 10, 64)
			if r1*d < r2*d {
				return true
			} else if r1*d > r2*d {
				return false
			}
		case "float32", "float64":
			d := float64(ov[i].direction)
			r1, _ := strconv.ParseFloat(ov[i].v[idx], 64)
			r2, _ := strconv.ParseFloat(ov[j].v[idx], 64)
			if r1*d < r2*d {
				return true
			} else if r1*d > r2*d {
				return false
			}
		}
	}
	return false
}

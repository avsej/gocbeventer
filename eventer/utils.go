package eventer

import (
	"sort"
)

func SortUint16s(arr []uint16) {
	// TODO: Fix this sorting shenangians
	arrInts := make([]int, len(arr))
	for i := range arr {
		arrInts[i] = int(arr[i])
	}
	sort.Ints(arrInts)
	for i := range arrInts {
		arr[i] = uint16(arrInts[i])
	}
}
package domain

import (
	"fmt"
	"sort"
	"strings"

	"github.com/mingyi850/repcrec/internal/utils"
)

/*
****
Custom Structs
****
*/
type HistoricalValue struct {
	value int
	time  int
}

func (h HistoricalValue) GetValue() int {
	return h.value
}

func (h HistoricalValue) GetTime() int {
	return h.time
}

type DataManager interface {
	Dump() string
	Read(key int, time int) HistoricalValue
	Commit(key int, value int, time int) error
	GetLastCommitted(key int) HistoricalValue
}

func CreateDataManager(siteId int) DataManagerImpl {
	result := DataManagerImpl{
		siteId:         siteId,
		commitedValues: initValuesMap(siteId),
	}
	return result
}

type DataManagerImpl struct {
	siteId         int
	commitedValues map[int][]HistoricalValue
}

func (d *DataManagerImpl) Dump() string {
	keys := getManagedKeys(d.siteId)
	sort.IntSlice(keys).Sort()
	values := make([]int, 0)
	for _, key := range keys {
		values = append(values, d.GetLastCommitted(key).value)
	}
	result := make([]string, 0)
	for i := 0; i < len(values); i++ {
		result = append(result, fmt.Sprintf("x%d: %d", keys[i], values[i]))
	}
	return fmt.Sprintf("site %d - %s", d.siteId, strings.Join(result, ", "))
}

func (d *DataManagerImpl) GetLastCommitted(key int) HistoricalValue {
	committedArray := d.commitedValues[key]
	return committedArray[len(committedArray)-1]
}

func (d *DataManagerImpl) Read(key int, time int) HistoricalValue {
	for i := len(d.commitedValues[key]) - 1; i >= 0; i-- {
		if d.commitedValues[key][i].time <= time {
			return d.commitedValues[key][i]
		}
	}
	return HistoricalValue{-1, -1}
}

func (d *DataManagerImpl) Commit(key int, value int, time int) error {
	d.commitedValues[key] = append(d.commitedValues[key], HistoricalValue{value, time})
	return nil
}

func initValuesMap(siteId int) map[int][]HistoricalValue {
	keyList := getManagedKeys(siteId)
	keys := make(map[int][]HistoricalValue)
	for _, key := range keyList {
		keys[key] = append(keys[key], initvalue(key))
	}
	return keys
}

func getManagedKeys(siteId int) []int {
	keys := utils.GetRange(2, 20, 2)
	if siteId%2 == 0 {
		keys = append(keys, siteId-1, siteId+9)
	}
	return keys
}

func initvalue(key int) HistoricalValue {
	return HistoricalValue{key * 10, -1}
}

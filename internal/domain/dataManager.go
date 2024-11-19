/**************************
File: dataManager.go
Author: Mingyi Lim
Description: This file contains the implementation of the DataManager interface. The DataManager is responsible for managing the data at a single site. It provides interfaces to access and modify the data.
***************************/

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

/*
Represents value of a key at this site and the time it was committed
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

/*
The Data Manager is responsible for managing the data at a single site. It provides interfaces to access and modify the data.
*/
type DataManager interface {
	Dump() string
	Read(key int, time int) HistoricalValue
	Commit(key int, value int, time int) error
	GetLastCommitted(key int) HistoricalValue
}

/* Each key contains a list of committed values */
type DataManagerImpl struct {
	siteId         int
	commitedValues map[int][]HistoricalValue
}

/* Creates and returns an instance of the DataManagerImpl */
func CreateDataManager(siteId int) DataManagerImpl {
	result := DataManagerImpl{
		siteId:         siteId,
		commitedValues: initValuesMap(siteId),
	}
	return result
}

/* Returns a single line representing a snapshot of all committed data at the site */
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

/* Returns the last committed value of a key at the current time */
func (d *DataManagerImpl) GetLastCommitted(key int) HistoricalValue {
	committedArray := d.commitedValues[key]
	return committedArray[len(committedArray)-1]
}

/* Returns the last committed value of a key at a given time */
func (d *DataManagerImpl) Read(key int, time int) HistoricalValue {
	for i := len(d.commitedValues[key]) - 1; i >= 0; i-- {
		if d.commitedValues[key][i].time <= time {
			return d.commitedValues[key][i]
		}
	}
	return HistoricalValue{-1, -1}
}

/* Commits a value to a key at a given time. Writes a new value to the committed values for the given key */
func (d *DataManagerImpl) Commit(key int, value int, time int) error {
	d.commitedValues[key] = append(d.commitedValues[key], HistoricalValue{value, time})
	return nil
}

/*
*******
Private Methods
*******
*/
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

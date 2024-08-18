package bigtable

import (
	"sort"
	"sync"
)


type Table struct {
    name    string
    tablets []*Tablet
    mutex   sync.RWMutex
}

func NewTable(name string) *Table {
    return &Table{
        name:    name,
        tablets: []*Tablet{NewTablet(name, "", "~")},
    }
}

func (t *Table) Write(row string, data []byte) error {
    t.mutex.RLock()
    tablet := t.findTablet(row)
    t.mutex.RUnlock()

    err := tablet.Write(row, data)
    if err != nil {
        return err
    }

    // Check if we need to split the tablet
    t.mutex.Lock()
    defer t.mutex.Unlock()
    if tablet.NeedsToSplit() {
        t.splitTablet(tablet)
    }

    return nil
}

func (t *Table) Read(row string) ([]byte, error) {
    t.mutex.RLock()
    tablet := t.findTablet(row)
    t.mutex.RUnlock()
    return tablet.Read(row)
}

func (t *Table) findTablet(row string) *Tablet {
    // Binary search to find the correct tablet
    i := sort.Search(len(t.tablets), func(i int) bool {
        return t.tablets[i].endKey > row || t.tablets[i].endKey == "~"
    })
    return t.tablets[i]
}

func (t *Table) splitTablet(tablet *Tablet) {
    midKey := tablet.FindMidKey()
    newTablet := NewTablet(t.name, midKey, tablet.endKey)
    tablet.endKey = midKey

    // Move half of the data to the new tablet
    for key, value := range tablet.data {
        if key >= midKey {
            newTablet.data[key] = value
            delete(tablet.data, key)
        }
    }

    // Insert the new tablet into the sorted list of tablets
    i := sort.Search(len(t.tablets), func(i int) bool {
        return t.tablets[i].startKey >= midKey
    })
    t.tablets = append(t.tablets[:i], append([]*Tablet{newTablet}, t.tablets[i:]...)...)
}
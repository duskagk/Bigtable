package bigtable


type ColumnFamily struct {
    name        string
    maxVersions int
}

func NewColumnFamily(name string, maxVersions int) *ColumnFamily {
    return &ColumnFamily{
        name:        name,
        maxVersions: maxVersions,
    }
}
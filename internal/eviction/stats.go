package eviction

var KeyspaceStats [8]map[string]int

func init() {
	// Initialize all 8 databases/maps so they aren't nil
	//unless goes to panic
	for i := 0; i < len(KeyspaceStats); i++ {
		KeyspaceStats[i] = make(map[string]int)
	}
}

func UpdateDBStat(num int, metric string, value int) {
	KeyspaceStats[num][metric] = value
}

package stationcluster

// consensusLogger prevents the upstream consensus library from projecting
// raw member/topology values into process logs. Errors still return through
// the owning loop; impossible fatal/panic paths retain fixed diagnostics.
type consensusLogger struct{}

func (consensusLogger) Debug(...interface{})            {}
func (consensusLogger) Debugf(string, ...interface{})   {}
func (consensusLogger) Error(...interface{})            {}
func (consensusLogger) Errorf(string, ...interface{})   {}
func (consensusLogger) Info(...interface{})             {}
func (consensusLogger) Infof(string, ...interface{})    {}
func (consensusLogger) Warning(...interface{})          {}
func (consensusLogger) Warningf(string, ...interface{}) {}
func (consensusLogger) Fatal(...interface{})            { panic("CLUSTER_CONSENSUS_FATAL") }
func (consensusLogger) Fatalf(string, ...interface{})   { panic("CLUSTER_CONSENSUS_FATAL") }
func (consensusLogger) Panic(...interface{})            { panic("CLUSTER_CONSENSUS_PANIC") }
func (consensusLogger) Panicf(string, ...interface{})   { panic("CLUSTER_CONSENSUS_PANIC") }

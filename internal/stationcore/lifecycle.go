package stationcore

type lifecycleState int

const (
	lifecycleStarting lifecycleState = iota + 1
	lifecycleRunning
	lifecycleDraining
	lifecycleStopped
)

type lifecycle struct {
	state lifecycleState
}

func newLifecycle() *lifecycle {
	return &lifecycle{state: lifecycleStarting}
}

func (life *lifecycle) startServing() {
	if life.state == lifecycleStarting {
		life.state = lifecycleRunning
	}
}

func (life *lifecycle) beginDrain() {
	if life.state == lifecycleRunning {
		life.state = lifecycleDraining
	}
}

func (life *lifecycle) stop() {
	life.state = lifecycleStopped
}

func (life *lifecycle) running() bool {
	return life.state == lifecycleRunning
}

func (life *lifecycle) draining() bool {
	return life.state == lifecycleDraining
}

func (life *lifecycle) acceptsAdmission() bool {
	return life.state == lifecycleRunning
}

func (life *lifecycle) acceptsClaim() bool {
	return life.state == lifecycleRunning
}

func (life *lifecycle) acceptsSettle() bool {
	return life.state == lifecycleRunning || life.state == lifecycleDraining
}

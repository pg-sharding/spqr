package port

type RouterPortType int

const (
	DefaultRouterPortType = RouterPortType(0)

	RORouterPortType = RouterPortType(1)

	ADMRouterPortType = RouterPortType(2)

	UnixSocketPortType = RouterPortType(3)
)

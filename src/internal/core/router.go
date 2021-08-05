package core

type router struct {
	routePool map[routeKey][]*route

	clPool clientPool

}
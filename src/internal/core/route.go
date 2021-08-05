package core


type routeKey struct {
	usr string
	db string
}

type route struct {
	rule Rule
	serv   ShServer
	client shClient
}
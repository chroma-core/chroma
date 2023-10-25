package common

// Compoent is the base class for difference compoents of the system
type Component interface {
	Start() error
	Stop() error
}

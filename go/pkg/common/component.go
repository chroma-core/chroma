package common

// Compoent is the base class for difference components of the system
type Component interface {
	Start() error
	Stop() error
}

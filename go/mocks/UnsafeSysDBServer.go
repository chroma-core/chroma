// Code generated by mockery v2.42.1. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// UnsafeSysDBServer is an autogenerated mock type for the UnsafeSysDBServer type
type UnsafeSysDBServer struct {
	mock.Mock
}

// mustEmbedUnimplementedSysDBServer provides a mock function with given fields:
func (_m *UnsafeSysDBServer) mustEmbedUnimplementedSysDBServer() {
	_m.Called()
}

// NewUnsafeSysDBServer creates a new instance of UnsafeSysDBServer. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewUnsafeSysDBServer(t interface {
	mock.TestingT
	Cleanup(func())
}) *UnsafeSysDBServer {
	mock := &UnsafeSysDBServer{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}

package views

import (
	"math"
	"testing"
)

func floatEquals(a, b float64) bool {
	return math.Abs(a-b) < 1e-6
}

func vectorEquals(a, b Vector3D) bool {
	return floatEquals(a.X, b.X) && floatEquals(a.Y, b.Y) && floatEquals(a.Z, b.Z)
}

func TestVectorOperations(t *testing.T) {
	a := Vector3D{1, 2, 3}
	b := Vector3D{4, 5, 6}

	c := Add(a, b)
	if c != (Vector3D{5, 7, 9}) {
		t.Errorf("Add failed: got %v", c)
	}
	d := Subtract(b, a)
	if d != (Vector3D{3, 3, 3}) {
		t.Errorf("Subtract failed: got %v", d)
	}
	s := Scale(a, 2)
	if s != (Vector3D{2, 4, 6}) {
		t.Errorf("Scale failed: got %v", s)
	}
	l := a.Length()
	if !floatEquals(l, math.Sqrt(14)) {
		t.Errorf("Length failed: got %v", l)
	}
	dot := Dot(a, b)
	if !floatEquals(dot, 32) {
		t.Errorf("Dot failed: got %v", dot)
	}
	n := Normalize(Vector3D{0, 3, 4})
	if !floatEquals(n.X, 0) || !floatEquals(n.Y, 0.6) || !floatEquals(n.Z, 0.8) {
		t.Errorf("Normalize failed: got %v", n)
	}
}

func TestMatrixTransform(t *testing.T) {
	m := Matrix{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	}
	v := Vector3D{1, 2, 3}
	res := m.Transform(v)
	if res != v {
		t.Errorf("Matrix.Transform failed: got %v", res)
	}
}

func TestCameraViewport(t *testing.T) {
	testCases := []struct {
		camera                    Camera
		expectedViewportUpperLeft Vector3D
		expectedViewpoerCenter    Vector3D
	}{
		{
			Camera{
				Position:       Vector3D{0, 0, 0},
				Direction:      Vector3D{0, 0, 1},
				Up:             Vector3D{0, 1, 0},
				ViewportWidth:  100,
				ViewportHeight: 100,
				FocalLength:    1,
			},
			Vector3D{50, 50, 1},
			Vector3D{0, 0, 1},
		},
		{
			Camera{
				Position:       Vector3D{10, 10, 10},
				Direction:      Vector3D{0, 0, 1},
				Up:             Normalize(Vector3D{1, 1, 0}),
				ViewportWidth:  1,
				ViewportHeight: 1,
				FocalLength:    1,
			},
			Vector3D{10.7071067812, 10, 11},
			Vector3D{10, 10, 11},
		},
	}
	for i, tc := range testCases {
		viewportUpperLeft := tc.camera.viewportUpperLeft()
		if !vectorEquals(viewportUpperLeft, tc.expectedViewportUpperLeft) {
			t.Errorf("Test %d: Camera.viewportUpperLeft not correct: %v", i, viewportUpperLeft)
		}
		viewportCenter := tc.camera.viewportCenter()
		if !vectorEquals(viewportCenter, tc.expectedViewpoerCenter) {
			t.Errorf("Test %d: Camera.viewportCenter not correct: %v", i, viewportCenter)
		}
	}
}

func TestCameraRayAt(t *testing.T) {
	cameraCenter := Vector3D{10, 10, 10}
	camera := Camera{
		Position:       cameraCenter,
		Direction:      Vector3D{0, 0, 1},
		Up:             Vector3D{0, 1, 0},
		ViewportWidth:  100,
		ViewportHeight: 100,
		FocalLength:    1,
	}
	tlRay := camera.viewportRayAt(0, 0)
	if !vectorEquals(tlRay.Origin, cameraCenter) || !vectorEquals(tlRay.Direction, Vector3D{50, 50, 1}) {
		t.Errorf("Camera.rayAt direction not correct: %v", tlRay.Direction)
	}
	trRay := camera.viewportRayAt(1, 0)
	if !vectorEquals(trRay.Origin, cameraCenter) || !vectorEquals(trRay.Direction, Vector3D{-50, 50, 1}) {
		t.Errorf("Camera.rayAt direction not correct: %v", trRay.Direction)
	}
	centerRay := camera.viewportRayAt(0.5, 0.5)
	if !vectorEquals(centerRay.Origin, cameraCenter) || !vectorEquals(centerRay.Direction, Vector3D{0, 0, 1}) {
		t.Errorf("Camera.rayAt direction not correct: %v", centerRay.Direction)
	}
	blRay := camera.viewportRayAt(0, 1)
	if !vectorEquals(blRay.Origin, cameraCenter) || !vectorEquals(blRay.Direction, Vector3D{50, -50, 1}) {
		t.Errorf("Camera.rayAt direction not correct: %v", blRay.Direction)
	}
	brRay := camera.viewportRayAt(1, 1)
	if !vectorEquals(brRay.Origin, cameraCenter) || !vectorEquals(brRay.Direction, Vector3D{-50, -50, 1}) {
		t.Errorf("Camera.rayAt direction not correct: %v", brRay.Direction)
	}
}

func TestIntersectsPlane(t *testing.T) {

	testCases := []struct {
		ray   Ray
		plane Plane
		pt    Vector3D
		ok    bool
	}{
		{
			ray:   Ray{Origin: Vector3D{0, 0, 0}, Direction: Vector3D{0, 0, 1}},
			plane: Plane{Point: Vector3D{0, 0, 0}, Normal: Vector3D{0, 0, 1}},
			pt:    Vector3D{0, 0, 0},
			ok:    true,
		},
		{
			ray:   Ray{Origin: Vector3D{0, 0, 0}, Direction: Vector3D{0, 0, 1}},
			plane: Plane{Point: Vector3D{0, 0, 2}, Normal: Vector3D{0, 0, 1}},
			pt:    Vector3D{0, 0, 2},
			ok:    true,
		},
		{
			ray:   Ray{Origin: Vector3D{0, 0, 0}, Direction: Vector3D{0, 0, 1}},
			plane: Plane{Point: Vector3D{0, 0, 2}, Normal: Vector3D{0, 0, -1}},
			pt:    Vector3D{0, 0, 2},
			ok:    true,
		},
		{
			ray:   Ray{Origin: Vector3D{1, 0, 1}, Direction: Vector3D{-1, 0, 1}},
			plane: Plane{Point: Vector3D{0, 0, 2}, Normal: Vector3D{0, 0, 1}},
			pt:    Vector3D{0, 0, 2},
			ok:    true,
		},
		{
			ray:   Ray{Origin: Vector3D{1, 0, 1}, Direction: Vector3D{-1, 0, 1}},
			plane: Plane{Point: Vector3D{0, 0, 2}, Normal: Vector3D{0, 0, 1}},
			pt:    Vector3D{0, 0, 2},
			ok:    true,
		},
		{
			ray:   Ray{Origin: Vector3D{0, 0, 1}, Direction: Vector3D{0, 0, 1}},
			plane: Plane{Point: Vector3D{0, 0, 0}, Normal: Vector3D{0, 1, 0}},
			ok:    false,
		},
	}

	for i, tc := range testCases {
		pt, ok := tc.plane.Intersects(tc.ray)
		point := tc.ray.At(pt)
		if ok != tc.ok {
			t.Errorf("Test %d: Unexpected intersection result: %v", i, ok)
		}
		if ok && !vectorEquals(point, tc.pt) {
			t.Errorf("Test %d: Ray.intersectsAt point not correct: %v", i, point)
		}
	}
}

func TestIntersectsTaurus(t *testing.T) {
	taurus := Taurus{
		Plane: Plane{Normal: Vector3D{0, 0, -1}, Point: Vector3D{0, 0, 10}},
		R1:    3,
		R2:    10,
	}
	rayCenter := Ray{Origin: Vector3D{0, 0, 0}, Direction: Normalize(Vector3D{0, 0, 10})}
	_, ok := taurus.Intersects(rayCenter)
	if ok {
		t.Errorf("Unexpected intersection result: %v", ok)
	}
	for i := 4; i <= 10; i++ {
		ray := Ray{Origin: Vector3D{0, 0, 0}, Direction: Normalize(Vector3D{0, float64(i), 10})}
		_, ok := taurus.Intersects(ray)
		if !ok {
			t.Errorf("Test %d: Unexpected intersection result: %v", i, ok)
		}
	}
	ray := Ray{Origin: Vector3D{0, 0, 0}, Direction: Normalize(Vector3D{0, 11, 10})}
	_, ok = taurus.Intersects(ray)
	if ok {
		t.Errorf("Unexpected intersection result: %v", ok)
	}
}

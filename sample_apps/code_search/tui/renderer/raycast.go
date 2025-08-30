/*
This is a simple raycast renderer that renders a scene to an ASCII art image.
*/

package renderer

import (
	"chroma-core/code-search-tui/util"
	"math"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/google/uuid"
)

// Right-handed coordinate system

type Vector2D struct {
	X float64
	Y float64
}

type Vector3D struct {
	X float64
	Y float64
	Z float64
}

func (v Vector3D) Length() float64 {
	return math.Sqrt(v.X*v.X + v.Y*v.Y + v.Z*v.Z)
}

func Dot(a, b Vector3D) float64 {
	return a.X*b.X + a.Y*b.Y + a.Z*b.Z
}

func Cross(a, b Vector3D) Vector3D {
	return Vector3D{
		a.Y*b.Z - a.Z*b.Y,
		a.Z*b.X - a.X*b.Z,
		a.X*b.Y - a.Y*b.X,
	}
}

func Subtract(a, b Vector3D) Vector3D {
	return Vector3D{a.X - b.X, a.Y - b.Y, a.Z - b.Z}
}

func Add(a, b Vector3D) Vector3D {
	return Vector3D{a.X + b.X, a.Y + b.Y, a.Z + b.Z}
}

func Scale(v Vector3D, s float64) Vector3D {
	return Vector3D{v.X * s, v.Y * s, v.Z * s}
}

func Normalize(v Vector3D) Vector3D {
	length := v.Length()
	return Vector3D{v.X / length, v.Y / length, v.Z / length}
}

type Matrix [3]Vector3D

func (m Matrix) Transform(v Vector3D) Vector3D {
	return Vector3D{
		Dot(m[0], v),
		Dot(m[1], v),
		Dot(m[2], v),
	}
}

func (v Vector3D) Rotate(axis Ray, radians float64) Vector3D {
	cos := math.Cos(radians)
	sin := math.Sin(radians)
	return Vector3D{
		X: v.X*cos - v.Z*sin,
		Y: v.Y,
		Z: v.X*sin + v.Z*cos,
	}
}

type Ray struct {
	Origin    Vector3D
	Direction Vector3D
}

func (r Ray) At(t float64) Vector3D {
	return Add(r.Origin, Scale(r.Direction, t))
}

type Surface interface {
	Intersects(ray Ray) (float64, bool)
}

type Plane struct {
	Point  Vector3D
	Normal Vector3D
}

func (plane Plane) Intersects(ray Ray) (float64, bool) {
	denom := Dot(plane.Normal, ray.Direction)
	if math.Abs(float64(denom)) < 1e-6 {
		return 0, false
	}
	numerator := Dot(Subtract(plane.Point, ray.Origin), plane.Normal)
	t := numerator / denom
	if t < 0 {
		return 0, false
	}
	return t, true
}

func (plane Plane) ProjectToPlane(v Vector3D) Vector3D {
	// Find the projection of A onto the normal direction. Then subtract that
	// projection from A. What is left is the projection of A onto the
	// orthogonal plane.
	projection := Scale(plane.Normal, Dot(v, plane.Normal))
	return Subtract(v, projection)
}

type TexturedPlane struct {
	Plane Plane
	Gap   float64
	Width float64
}

func (tp TexturedPlane) Intersects(ray Ray) (float64, bool) {
	t, ok := tp.Plane.Intersects(ray)
	if !ok {
		return 0, false
	}
	posOnPlane := tp.Plane.ProjectToPlane(ray.At(t))
	ok = math.Abs(math.Mod(posOnPlane.X, tp.Gap)) < tp.Width
	return t, ok
}

type Taurus struct {
	Plane Plane
	R1    float64
	R2    float64
}

func (taurus Taurus) Intersects(ray Ray) (float64, bool) {
	t, ok := taurus.Plane.Intersects(ray)
	if !ok {
		return 0, false
	}
	distance := Subtract(ray.At(t), taurus.Plane.Point).Length()
	ok = distance >= taurus.R1 && distance <= taurus.R2
	return t, ok
}

type TaurusArc struct {
	Plane Plane
	R1    float64
	R2    float64
	Start float64
	End   float64
}

func (ta TaurusArc) Intersects(ray Ray) (float64, bool) {
	t, ok := ta.Plane.Intersects(ray)
	if !ok {
		return 0, false
	}
	p := ray.At(t)
	distance := Subtract(p, ta.Plane.Point).Length()
	ok = distance >= ta.R1 && distance <= ta.R2
	p2 := Subtract(p, ta.Plane.Point)
	angle := math.Atan2(p2.Y, p2.X)
	if angle < 0 {
		angle += 2 * math.Pi
	}
	ok = ok && angle >= ta.Start && angle <= ta.End
	return t, ok
}

type Camera struct {
	Position       Vector3D
	Direction      Vector3D
	Up             Vector3D
	FocalLength    float64
	ViewportWidth  float64
	ViewportHeight float64
}

func (camera Camera) viewportEdgeVectors() (Vector3D, Vector3D) {
	down := Scale(camera.Up, -camera.ViewportHeight)
	right := Scale(Cross(camera.Direction, camera.Up), camera.ViewportWidth)
	return right, down
}

func (camera Camera) viewportCenter() Vector3D {
	return Add(camera.Position, Scale(camera.Direction, camera.FocalLength))
}

func (camera Camera) viewportUpperLeft() Vector3D {
	u, v := camera.viewportEdgeVectors()
	offset := Add(Scale(u, 0.5), Scale(v, 0.5))
	return Subtract(camera.viewportCenter(), offset)
}

func (camera Camera) viewportRayAt(x, y float64) Ray {
	if x < 0 || x > 1 || y < 0 || y > 1 {
		panic("x and y must be between 0 and 1")
	}
	tl := camera.viewportUpperLeft()
	right, down := camera.viewportEdgeVectors()
	direction := Subtract(Add(Add(Scale(right, x), Scale(down, y)), tl), camera.Position)
	return Ray{camera.Position, direction}
}

type SceneObject struct {
	Surface Surface
	Color   string
}

var luminance = "@$#*!=;:~-,."

func (camera Camera) Render(s SceneObject, outputBuffer [][]ASCIIPixel) {
	W, H := float64(len(outputBuffer[0])), float64(len(outputBuffer))
	for i := range outputBuffer {
		for j := range outputBuffer[i] {
			x := float64(j) / W
			y := float64(i) / H
			ray := camera.viewportRayAt(x, y)
			distance, intersects := s.Surface.Intersects(ray)
			if intersects {
				index := int(math.Max(distance*2-14, 0))
				index = int(math.Min(float64(index), float64(len(luminance)-1)))
				outputBuffer[i][j] = ASCIIPixel{Color: s.Color, Char: rune(luminance[index])}
			}
		}
	}
}

type Scene struct {
	Objects []SceneObject
	Camera  Camera
	Update  func(scene Scene, context RaycastSceneContext, msg tea.Msg) (Scene, tea.Cmd)
}

func (scene Scene) Render(outputBuffer [][]ASCIIPixel) {
	for _, object := range scene.Objects {
		scene.Camera.Render(object, outputBuffer)
	}
}

type RaycastSceneContext struct {
	Id           string
	ScreenWidth  int
	ScreenHeight int
	MouseX       float32
	MouseY       float32
}

type RaycastSceneModel struct {
	Context RaycastSceneContext
	Scenes  []Scene
}

func NewRaycastSceneModel(scenes []Scene) RaycastSceneModel {
	return RaycastSceneModel{
		Context: RaycastSceneContext{
			Id: uuid.New().String(),
		},
		Scenes: scenes,
	}
}

func (m RaycastSceneModel) Init() tea.Cmd {
	return func() tea.Msg {
		return util.TickMsg{Id: m.Context.Id}
	}
}

func (m RaycastSceneModel) View(outputBuffer [][]ASCIIPixel) {
	aspectRatio := float64(len(outputBuffer[0])) / float64(len(outputBuffer))
	for _, scene := range m.Scenes {
		// Adjust viewport width for terminal character aspect ratio
		scene.Camera.ViewportWidth = aspectRatio * scene.Camera.ViewportHeight / 2.5
		scene.Render(outputBuffer)
	}
}

func (m RaycastSceneModel) Update(msg tea.Msg) (RaycastSceneModel, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.Context.ScreenWidth = msg.Width
		m.Context.ScreenHeight = msg.Height
	case tea.MouseMsg:
		m.Context.MouseX = float32(msg.X) / float32(m.Context.ScreenWidth+1)
		m.Context.MouseY = float32(msg.Y) / float32(m.Context.ScreenHeight+1)
	}

	for i, scene := range m.Scenes {
		if scene.Update != nil {
			scene, cmd := scene.Update(scene, m.Context, msg)
			m.Scenes[i] = scene
			cmds = append(cmds, cmd)
		}
	}

	return m, tea.Batch(cmds...)
}

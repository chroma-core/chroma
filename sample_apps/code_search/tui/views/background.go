/*
This is a simple raycast renderer that renders a scene to an ASCII art image.
*/

package views

import (
	"fmt"
	"math"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
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

func (v Vector3D) Rotate(_ Ray, radians float64) Vector3D {
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

func hexToANSIEscapeCode(hex string) string {
	// Remove leading '#' if present
	if len(hex) > 0 && hex[0] == '#' {
		hex = hex[1:]
	}
	// Only support 6-digit hex
	if len(hex) != 6 {
		return ""
	}
	var r, g, b uint8
	_, err := fmt.Sscanf(hex, "%02x%02x%02x", &r, &g, &b)
	if err != nil {
		return ""
	}
	// 38;2;r;g;b for foreground, 48;2;r;g;b for background
	return fmt.Sprintf("\x1b[38;2;%d;%d;%dm", r, g, b)
}

var (
	blue   = hexToANSIEscapeCode("#327EFF")
	red    = hexToANSIEscapeCode("#FF6446")
	yellow = hexToANSIEscapeCode("#FFDE2C")
	gray   = hexToANSIEscapeCode("#808080")
)

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
				index := int(math.Max(distance*2-8, 0))
				index = int(math.Min(float64(index), float64(len(luminance)-1)))
				outputBuffer[i][j] = ASCIIPixel{Color: s.Color, Char: rune(luminance[index])}
			}
		}
	}
}

var (
	backgroundPlanes = []SceneObject{
		{
			Surface: TexturedPlane{
				Plane: Plane{Normal: Vector3D{0, 1, .15}, Point: Vector3D{0, -3, 0}},
				Gap:   1,
				Width: .1,
			},
			Color: gray,
		},
		{
			Surface: TexturedPlane{
				Plane: Plane{Normal: Vector3D{0, 1, -.15}, Point: Vector3D{0, 3, 0}},
				Gap:   1,
				Width: .1,
			},
			Color: gray,
		},
	}
	chromaLogo = []SceneObject{
		{
			Surface: Taurus{
				Plane: Plane{Normal: Vector3D{0, 0, 1}, Point: Vector3D{-.5, 0, 0}},
				R1:    0,
				R2:    1,
			},
			Color: blue,
		},
		{
			Surface: Taurus{
				Plane: Plane{Normal: Vector3D{0, 0, 1}, Point: Vector3D{.5, 0, 0}},
				R1:    0,
				R2:    1,
			},
			Color: yellow,
		},
		{
			Surface: TaurusArc{
				Plane: Plane{Normal: Vector3D{0, 0, 1}, Point: Vector3D{.5, 0, 0}},
				R1:    0,
				R2:    1,
				Start: math.Pi / 2,
				End:   math.Pi,
			},
			Color: red,
		},
		{
			Surface: TaurusArc{
				Plane: Plane{Normal: Vector3D{0, 0, 1}, Point: Vector3D{-.5, 0, 0}},
				R1:    0,
				R2:    1,
				Start: 3 * math.Pi / 2,
				End:   math.Pi * 2,
			},
			Color: red,
		},
	}
)

type Scene struct {
	Objects []SceneObject
	Camera  Camera
}

func (scene Scene) Render(outputBuffer [][]ASCIIPixel) {
	for _, object := range scene.Objects {
		scene.Camera.Render(object, outputBuffer)
	}
}

type LogoModel struct {
	Width  int
	Height int
	Scenes []Scene
	camera Camera
	scene  []SceneObject
	frame  int
}

func NewLogoModel() LogoModel {
	return LogoModel{
		Width:  64,
		Height: 32,
		Scenes: []Scene{},
		camera: Camera{
			Position:       Vector3D{0, 0, -5},
			Direction:      Vector3D{0, 0, 1},
			Up:             Vector3D{0, 1, 0},
			FocalLength:    1,
			ViewportWidth:  1,
			ViewportHeight: 1,
		},
		scene: append(backgroundPlanes, chromaLogo...),
	}
}

func (m LogoModel) Init() tea.Cmd {
	return nil
}

type ASCIIPixel struct {
	Color string
	Char  rune
}

func (m LogoModel) View() string {
	outputBuffer := make([][]ASCIIPixel, m.Height)
	for i := range outputBuffer {
		outputBuffer[i] = make([]ASCIIPixel, m.Width)
		for j := range outputBuffer[i] {
			outputBuffer[i][j] = ASCIIPixel{Color: "#000000", Char: ' '}
		}
	}

	for _, sceneObject := range m.scene {
		m.camera.Render(sceneObject, outputBuffer)
	}

	var sb strings.Builder
	sb.Grow(m.Width * m.Height * 10) // Pre-allocate approximate capacity
	for _, row := range outputBuffer {
		for c, pixel := range row {
			if pixel.Char != ' ' && (c == 0 || row[c-1].Color != pixel.Color) {
				sb.WriteString(pixel.Color)
			}
			sb.WriteRune(pixel.Char)
		}
		sb.WriteString("\x1b[0m\n")
	}
	return sb.String()
}

type TickMsg struct{}

func (m LogoModel) Update(msg tea.Msg) (LogoModel, tea.Cmd) {
	var cmds []tea.Cmd
	switch msg := msg.(type) {
	case TickMsg:
		m.frame++
		cmds = append(cmds, tea.Tick(time.Second/60, func(_ time.Time) tea.Msg {
			return TickMsg{}
		}))
		upRay := Ray{Origin: m.camera.Position, Direction: m.camera.Up}
		m.camera.Position = m.camera.Position.Rotate(upRay, .005)
		target := Vector3D{X: 0, Z: 0, Y: m.camera.Position.Y}
		m.camera.Direction = Normalize(Subtract(target, m.camera.Position))
	case tea.MouseMsg:
		multiplier := .2
		x := multiplier*float64(msg.X)/float64(m.Width) - multiplier/2
		y := multiplier*float64(msg.Y)/float64(m.Height) - multiplier/2
		m.camera.Position = Vector3D{X: x, Y: y, Z: m.camera.Position.Z}
	}

	return m, tea.Batch(cmds...)
}

package views

import (
	"chroma-core/code-search-tui/renderer"
	"chroma-core/code-search-tui/util"
	"math"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

var (
	backgroundPlanes = []renderer.SceneObject{
		{
			Surface: renderer.TexturedPlane{
				Plane: renderer.Plane{Normal: renderer.Vector3D{X: 0, Y: 1, Z: .15}, Point: renderer.Vector3D{X: 0, Y: -3, Z: 0}},
				Gap:   1,
				Width: .2,
			},
			Color: renderer.Gray,
		},
		{
			Surface: renderer.TexturedPlane{
				Plane: renderer.Plane{Normal: renderer.Vector3D{X: 0, Y: 1, Z: -.15}, Point: renderer.Vector3D{X: 0, Y: 3, Z: 0}},
				Gap:   1,
				Width: .2,
			},
			Color: renderer.Gray,
		},
	}
	chromaLogo = []renderer.SceneObject{
		{
			Surface: renderer.Taurus{
				Plane: renderer.Plane{Normal: renderer.Vector3D{X: 0, Y: 0, Z: 1}, Point: renderer.Vector3D{X: -.5, Y: 0, Z: 0}},
				R1:    0,
				R2:    1,
			},
			Color: renderer.Blue,
		},
		{
			Surface: renderer.Taurus{
				Plane: renderer.Plane{Normal: renderer.Vector3D{X: 0, Y: 0, Z: 1}, Point: renderer.Vector3D{X: .5, Y: 0, Z: 0}},
				R1:    0,
				R2:    1,
			},
			Color: renderer.Yellow,
		},
		{
			Surface: renderer.TaurusArc{
				Plane: renderer.Plane{Normal: renderer.Vector3D{X: 0, Y: 0, Z: 1}, Point: renderer.Vector3D{X: .5, Y: 0, Z: 0}},
				R1:    0,
				R2:    1,
				Start: math.Pi / 2,
				End:   math.Pi,
			},
			Color: renderer.Red,
		},
		{
			Surface: renderer.TaurusArc{
				Plane: renderer.Plane{Normal: renderer.Vector3D{X: 0, Y: 0, Z: 1}, Point: renderer.Vector3D{X: -.5, Y: 0, Z: 0}},
				R1:    0,
				R2:    1,
				Start: 3 * math.Pi / 2,
				End:   math.Pi * 2,
			},
			Color: renderer.Red,
		},
	}
)

func NewBackgroundModel() renderer.RaycastSceneModel {
	return renderer.NewRaycastSceneModel(
		[]renderer.Scene{
			{
				Objects: backgroundPlanes,
				Camera: renderer.Camera{
					Position:       renderer.Vector3D{X: 0, Y: 0, Z: 0},
					Direction:      renderer.Vector3D{X: 0, Y: 0, Z: 1},
					Up:             renderer.Vector3D{X: 0, Y: 1, Z: 0},
					FocalLength:    1,
					ViewportWidth:  1,
					ViewportHeight: 1,
				},
				Update: UpdateBackground,
			},
			{
				Objects: chromaLogo,
				Camera: renderer.Camera{
					Position:       renderer.Vector3D{X: 0, Y: -1, Z: -7},
					Direction:      renderer.Vector3D{X: 0, Y: 0, Z: 1},
					Up:             renderer.Vector3D{X: 0, Y: 1, Z: 0},
					FocalLength:    1,
					ViewportWidth:  1,
					ViewportHeight: 1,
				},
				Update: UpdateForeground,
			},
		},
	)
}

func UpdateBackground(scene renderer.Scene, context renderer.RaycastSceneContext, msg tea.Msg) (renderer.Scene, tea.Cmd) {
	cam := scene.Camera
	switch msg.(type) {
	case tea.MouseMsg:
		multiplier := .5
		x := multiplier*float64(context.MouseX) - multiplier/2
		y := multiplier*float64(context.MouseY) - multiplier/2
		cam.Position = renderer.Vector3D{X: x, Y: y, Z: cam.Position.Z}
		scene.Camera = cam
	}
	return scene, nil
}

const (
	StateDefault = iota
	StateSpinning
	StateSpinning2
)

var (
	spinState         = 0
	followMouse       = false
	followMouseVector = renderer.Vector3D{X: 0, Y: 0, Z: 0}
	startTime         = 0
)

func UpdateForeground(scene renderer.Scene, context renderer.RaycastSceneContext, msg tea.Msg) (renderer.Scene, tea.Cmd) {
	var cmds []tea.Cmd
	cam := scene.Camera
	switch msg := msg.(type) {
	case tea.KeyMsg:
		var startedAnimation = false
		switch msg.String() {
		case "ctrl+s":
			spinState = StateSpinning
			startTime = time.Now().Nanosecond()
			startedAnimation = true
		case "ctrl+d":
			spinState = StateSpinning2
			startTime = time.Now().Nanosecond()
		case "ctrl+f":
			followMouse = !followMouse
			startedAnimation = true
		}
		if startedAnimation {
			cmds = append(cmds, tea.Tick(time.Second/30, func(_ time.Time) tea.Msg {
				return util.TickMsg{Id: context.Id}
			}))
		}
	case util.TickMsg:
		cmds = append(cmds, tea.Tick(time.Second/30, func(_ time.Time) tea.Msg {
			return util.TickMsg{Id: msg.Id}
		}))
		var delta float64
		if spinState == StateSpinning {
			delta = .1
		} else if spinState == StateSpinning2 {
			delta = float64(time.Now().Nanosecond()-startTime) / 1e9
		}
		upRay := renderer.Ray{Origin: cam.Position, Direction: cam.Up}
		cam.Position = cam.Position.Rotate(upRay, delta)
		target := renderer.Vector3D{X: 0, Z: 0, Y: cam.Position.Y}
		cam.Direction = renderer.Normalize(renderer.Subtract(target, cam.Position))
		scene.Camera = cam

		if followMouse {
			followMouseVector.X = float64(context.MouseX) - .5
			followMouseVector.Y = float64(context.MouseY) - .5
			cam.Position = renderer.Add(cam.Position, followMouseVector)
			cam.Direction = renderer.Normalize(renderer.Subtract(target, cam.Position))
			scene.Camera = cam
		}

	}
	return scene, tea.Batch(cmds...)
}

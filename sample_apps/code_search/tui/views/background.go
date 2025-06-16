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

func UpdateBackground(scene renderer.Scene, msg tea.Msg) (renderer.Scene, tea.Cmd) {
	cam := scene.Camera
	switch msg := msg.(type) {
	case tea.MouseMsg:
		multiplier := .2
		x := multiplier*float64(msg.X)/float64(scene.Camera.ViewportWidth) - multiplier/2
		y := multiplier*float64(msg.Y)/float64(scene.Camera.ViewportHeight) - multiplier/2
		cam.Position = renderer.Vector3D{X: x, Y: y, Z: cam.Position.Z}
		scene.Camera = cam
	}
	return scene, nil
}

func UpdateForeground(scene renderer.Scene, msg tea.Msg) (renderer.Scene, tea.Cmd) {
	var cmds []tea.Cmd
	cam := scene.Camera
	switch msg := msg.(type) {
	case util.TickMsg:
		cmds = append(cmds, tea.Tick(time.Second/60, func(_ time.Time) tea.Msg {
			return util.TickMsg{Id: msg.Id}
		}))
		upRay := renderer.Ray{Origin: cam.Position, Direction: cam.Up}
		cam.Position = cam.Position.Rotate(upRay, .005)
		target := renderer.Vector3D{X: 0, Z: 0, Y: cam.Position.Y}
		cam.Direction = renderer.Normalize(renderer.Subtract(target, cam.Position))
		scene.Camera = cam
	}
	return scene, tea.Batch(cmds...)
}

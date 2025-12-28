package renderer

import "fmt"

type Focusable interface {
	Focus()
	Blur()
	IsFocused() bool
}

func NewBuffer(width, height int) [][]ASCIIPixel {
	if width < 1 || height < 1 {
		panic(fmt.Sprintf("width and height must be greater than 0, got %d and %d", width, height))
	}
	outputBuffer := make([][]ASCIIPixel, height)
	for i := range outputBuffer {
		outputBuffer[i] = make([]ASCIIPixel, width)
		for j := range outputBuffer[i] {
			outputBuffer[i][j] = ASCIIPixel{Color: NullColor, Char: ' '}
		}
	}
	return outputBuffer
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
	NullColor = ""
	Blue      = hexToANSIEscapeCode("#327EFF")
	Red       = hexToANSIEscapeCode("#FF6446")
	Yellow    = hexToANSIEscapeCode("#FFDE2C")
	Gray      = hexToANSIEscapeCode("#808080")
	White     = hexToANSIEscapeCode("#FFFFFF")
	Black     = hexToANSIEscapeCode("#000000")
)

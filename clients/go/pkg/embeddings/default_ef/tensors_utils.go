//go:build unix

package defaultef

import (
	"errors"
	"fmt"
	"math"
)

// Number is a constraint that permits any number type
type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

type Tensor2D[T Number] [][]T

// Tensor3D is a generic 3D tensor
type Tensor3D[T Number] [][][]T

// ConvertTensor2D converts a Tensor2D of one numeric type to another
func ConvertTensor2D[S Number, D Number](src Tensor2D[S]) Tensor2D[D] {
	dst := make(Tensor2D[D], len(src))
	for i := range src {
		dst[i] = make([]D, len(src[i]))
		for j := range src[i] {
			dst[i][j] = D(src[i][j])
		}
	}
	return dst
}

// ConvertTensor3D converts a Tensor3D of one numeric type to another
func ConvertTensor3D[S Number, D Number](src Tensor3D[S]) Tensor3D[D] {
	dst := make(Tensor3D[D], len(src))
	for i := range src {
		dst[i] = make([][]D, len(src[i]))
		for j := range src[i] {
			dst[i][j] = make([]D, len(src[i][j]))
			for k := range src[i][j] {
				dst[i][j][k] = D(src[i][j][k])
			}
		}
	}
	return dst
}

// Sum calculates the sum along a specified axis
func (t Tensor3D[T]) Sum(axis int) ([][]T, error) {
	if len(t) == 0 || len(t[0]) == 0 || len(t[0][0]) == 0 {
		return nil, errors.New("empty tensor")
	}

	shape := []int{len(t), len(t[0]), len(t[0][0])}

	switch axis {
	case 0:
		result := make([][]T, shape[1])
		for i := range result {
			result[i] = make([]T, shape[2])
		}
		for i := 0; i < shape[1]; i++ {
			for j := 0; j < shape[2]; j++ {
				var sum T
				for k := 0; k < shape[0]; k++ {
					sum += t[k][i][j]
				}
				result[i][j] = sum
			}
		}
		return result, nil
	case 1:
		result := make([][]T, shape[0])
		for i := range result {
			result[i] = make([]T, shape[2])
		}
		for i := 0; i < shape[0]; i++ {
			for j := 0; j < shape[2]; j++ {
				var sum T
				for k := 0; k < shape[1]; k++ {
					sum += t[i][k][j]
				}
				result[i][j] = sum
			}
		}
		return result, nil
	case 2:
		result := make([][]T, shape[0])
		for i := range result {
			result[i] = make([]T, shape[1])
		}
		for i := 0; i < shape[0]; i++ {
			for j := 0; j < shape[1]; j++ {
				var sum T
				for k := 0; k < shape[2]; k++ {
					sum += t[i][j][k]
				}
				result[i][j] = sum
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("invalid axis: %d", axis)
	}
}

func multiply[T, U Number](a Tensor3D[T], b Tensor3D[U]) (Tensor3D[T], error) {
	// Check dimensions
	if len(a) != len(b) || len(a[0]) != len(b[0]) || len(a[0][0]) != len(b[0][0]) {
		return nil, errors.New("tensor dimensions are not compatible for element-wise multiplication")
	}

	// Perform multiplication
	result := make(Tensor3D[T], len(a))
	for i := range a {
		result[i] = make([][]T, len(a[i]))
		for j := range a[i] {
			result[i][j] = make([]T, len(a[i][j]))
			for k := range a[i][j] {
				result[i][j][k] = T(float64(a[i][j][k]) * float64(U(b[i][j][k])))
			}
		}
	}

	return result, nil
}

// clip applies the clip operation to a Tensor2D
func clip[T Number](input Tensor2D[T], min, max T) Tensor2D[T] {
	rows := len(input)
	if rows == 0 {
		return input
	}
	cols := len(input[0])

	result := make(Tensor2D[T], rows)
	for i := range result {
		result[i] = make([]T, cols)
	}

	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			result[i][j] = clipValue(input[i][j], min, max)
		}
	}

	return result
}

// clipValue clips a single value between min and max
func clipValue[T Number](x, min, max T) T {
	if x < min {
		return min
	}
	if x > max {
		return max
	}
	return x
}

// divide performs element-wise division of tensor a by tensor b
// It supports broadcasting and handles division by zero similar to NumPy
func divide[T ~float32 | ~float64](a, b Tensor2D[T]) Tensor2D[T] {
	rowsA, colsA := len(a), len(a[0])
	rowsB, colsB := len(b), len(b[0])

	// Determine output shape based on broadcasting rules
	rowsOut, colsOut := max(rowsA, rowsB), max(colsA, colsB)

	result := make(Tensor2D[T], rowsOut)
	for i := range result {
		result[i] = make([]T, colsOut)
	}

	for i := 0; i < rowsOut; i++ {
		for j := 0; j < colsOut; j++ {
			aVal := a[i%rowsA][j%colsA]
			bVal := b[i%rowsB][j%colsB]
			result[i][j] = divideValues(aVal, bVal)
		}
	}

	return result
}

// divideValues performs division for a single pair of values
func divideValues[T ~float32 | ~float64](a, b T) T {
	if b == 0 {
		switch {
		case a > 0:
			return T(math.Inf(1))
		case a < 0:
			return T(math.Inf(-1))
		default:
			return T(math.NaN())
		}
	}
	return T(float64(a) / float64(b))
}

func ReshapeFlattenedTensor[T Number](flatTensor []T, shape []int) (interface{}, error) {
	// Check if the shape is valid (2D or 3D)
	if len(shape) != 2 && len(shape) != 3 {
		return nil, errors.New("shape must be 2D or 3D")
	}

	// Calculate total elements based on shape
	totalElements := 1
	for _, dim := range shape {
		totalElements *= dim
	}

	// Check if the input slice has the correct number of elements
	if len(flatTensor) != totalElements {
		return nil, errors.New("input slice length does not match the specified shape")
	}

	if len(shape) == 2 {
		// Handle 2D case
		tensor := make(Tensor2D[T], shape[0])
		for i := range tensor {
			tensor[i] = make([]T, shape[1])
		}

		index := 0
		for i := 0; i < shape[0]; i++ {
			for j := 0; j < shape[1]; j++ {
				tensor[i][j] = flatTensor[index]
				index++
			}
		}
		return tensor, nil
	} else {
		// Handle 3D case
		tensor := make(Tensor3D[T], shape[0])
		for i := range tensor {
			tensor[i] = make([][]T, shape[1])
			for j := range tensor[i] {
				tensor[i][j] = make([]T, shape[2])
			}
		}

		index := 0
		for i := 0; i < shape[0]; i++ {
			for j := 0; j < shape[1]; j++ {
				for k := 0; k < shape[2]; k++ {
					tensor[i][j][k] = flatTensor[index]
					index++
				}
			}
		}
		return tensor, nil
	}
}

func ExpandDims(input []int64, shape []int64) ([][][]int64, error) {
	// Calculate the total size of the input
	var totalSize int64 = 1
	for _, dim := range shape {
		totalSize *= dim
	}

	// Check if the input size matches the shape
	if int64(len(input)) != totalSize {
		return nil, errors.New("input slice length does not match the specified shape")
	}

	// Reshape the input according to the given shape
	reshaped := make([][]int64, shape[0])
	for i := range reshaped {
		reshaped[i] = make([]int64, shape[1])
		for j := range reshaped[i] {
			reshaped[i][j] = input[i*int(shape[1])+j]
		}
	}

	// Add the extra dimension
	output := make([][][]int64, shape[0])
	for i := range output {
		output[i] = make([][]int64, shape[1])
		for j := range output[i] {
			output[i][j] = []int64{reshaped[i][j]}
		}
	}

	return output, nil
}

// BroadcastTo simulates np.broadcast_to for any 3D tensor
func BroadcastTo[T Number](input Tensor3D[T], targetShape [3]int) Tensor3D[T] {
	result := make(Tensor3D[T], targetShape[0])
	for i := range result {
		result[i] = make([][]T, targetShape[1])
		for j := range result[i] {
			result[i][j] = make([]T, targetShape[2])
			for k := range result[i][j] {
				// Use modulo to wrap around input dimensions
				iIn := i % len(input)
				jIn := j % len(input[iIn])
				kIn := k % len(input[iIn][jIn])
				result[i][j][k] = input[iIn][jIn][kIn]
			}
		}
	}
	return result
}

// normalize function for a generic Tensor2D type.
func normalize[T Number](v Tensor2D[T]) Tensor2D[float32] {
	rows := len(v)
	cols := len(v[0])
	norm := make([]float32, rows)

	// Step 1: Compute the L2 norm of each row
	for i := 0; i < rows; i++ {
		sum := 0.0
		for j := 0; j < cols; j++ {
			sum += float64(v[i][j]) * float64(v[i][j])
		}
		norm[i] = float32(math.Sqrt(sum))
	}

	// Step 2: Handle zero norms
	for i := 0; i < rows; i++ {
		if norm[i] == 0 {
			norm[i] = 1e-12
		}
	}

	// Step 3: Normalize each row
	normalized := make(Tensor2D[float32], rows)
	for i := 0; i < rows; i++ {
		normalized[i] = make([]float32, cols)
		for j := 0; j < cols; j++ {
			normalized[i][j] = float32(v[i][j]) / norm[i]
		}
	}

	return normalized
}

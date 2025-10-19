#!/bin/bash

set -eu

if [ $# -lt 1 ]
then
    echo "usage:   $0 path_to_build_binary [path_to_temp_folder]"
    echo "example: $0 ../../build/bin ../../tmp"
    exit 1
fi

if [ $# -gt 1 ]
then
    TMP_DIR=$2
else
    TMP_DIR=/tmp
fi

set -x

SPLIT=$1/llama-gguf-split
MAIN=$1/llama-cli
WORK_PATH=$TMP_DIR/gguf-split
ROOT_DIR=$(realpath $(dirname $0)/../../)

mkdir -p "$WORK_PATH"

# Clean up in case of previously failed test
rm -f $WORK_PATH/ggml-model-split*.gguf $WORK_PATH/ggml-model-merge*.gguf

# 1. Get a model
(
cd $WORK_PATH
"$ROOT_DIR"/scripts/hf.sh --repo ggml-org/gemma-1.1-2b-it-Q8_0-GGUF --file gemma-1.1-2b-it.Q8_0.gguf
)
echo PASS

# 2. Split with max tensors strategy
$SPLIT --split-max-tensors 28  $WORK_PATH/gemma-1.1-2b-it.Q8_0.gguf $WORK_PATH/ggml-model-split
echo PASS
echo

# 2b. Test the sharded model is loading properly
$MAIN --model $WORK_PATH/ggml-model-split-00001-of-00006.gguf --n-predict 32
echo PASS
echo

# 3. Merge
$SPLIT --merge $WORK_PATH/ggml-model-split-00001-of-00006.gguf $WORK_PATH/ggml-model-merge.gguf
echo PASS
echo

# 3b. Test the merged model is loading properly
$MAIN --model $WORK_PATH/ggml-model-merge.gguf --n-predict 32
echo PASS
echo

# 4. Split with no tensors in the first split
$SPLIT --split-max-tensors 32 --no-tensor-first-split $WORK_PATH/ggml-model-merge.gguf $WORK_PATH/ggml-model-split-32-tensors
echo PASS
echo

# 4b. Test the sharded model is loading properly
$MAIN --model $WORK_PATH/ggml-model-split-32-tensors-00001-of-00007.gguf --n-predict 32
echo PASS
echo

# 5. Merge
#$SPLIT --merge $WORK_PATH/ggml-model-split-32-tensors-00001-of-00006.gguf $WORK_PATH/ggml-model-merge-2.gguf
#echo PASS
#echo

# 5b. Test the merged model is loading properly
#$MAIN --model $WORK_PATH/ggml-model-merge-2.gguf --n-predict 32
#echo PASS
#echo

# 6. Split with size strategy
$SPLIT --split-max-size 2G $WORK_PATH/ggml-model-merge.gguf $WORK_PATH/ggml-model-split-2G
echo PASS
echo

# 6b. Test the sharded model is loading properly
$MAIN --model $WORK_PATH/ggml-model-split-2G-00001-of-00002.gguf --n-predict 32
echo PASS
echo

# Clean up
rm -f $WORK_PATH/ggml-model-split*.gguf $WORK_PATH/ggml-model-merge*.gguf

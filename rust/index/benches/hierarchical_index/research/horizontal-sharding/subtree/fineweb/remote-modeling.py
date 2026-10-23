# coding=utf-8
# Copyright 2024 The GTE Team Authors and Alibaba Group.
# Copyright (c) 2018, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""PyTorch NEW model."""

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple, Union

import torch
import torch.utils.checkpoint
from torch import nn

from transformers.activations import ACT2FN
from transformers.modeling_outputs import (
    BaseModelOutput,
    BaseModelOutputWithPooling,
    MaskedLMOutput,
    MultipleChoiceModelOutput,
    QuestionAnsweringModelOutput,
    SequenceClassifierOutput,
    ModelOutput,
)
from transformers.modeling_utils import PreTrainedModel
from transformers.utils import logging

try:
    import xformers.ops as xops
except ImportError as e:
    xops = None

from .configuration import NewConfig


logger = logging.get_logger(__name__)


# Adapted from https://github.com/HazyResearch/flash-attention/blob/main/flash_attn/bert_padding.py
# Which was adapted from https://github.com/mlcommons/training_results_v1.1/blob/main/NVIDIA/benchmarks/bert/implementations/pytorch/padding.py
class IndexFirstAxis(torch.autograd.Function):
    @staticmethod
    def forward(ctx, input, indices):
        ctx.save_for_backward(indices)
        assert input.ndim >= 2
        ctx.first_axis_dim, other_shape = input.shape[0], input.shape[1:]
        second_dim = other_shape.numel()
        # TD [2022-03-04] For some reason torch.gather is a bit faster than indexing.
        # return input[indices]
        # return torch.gather(
        #     rearrange(input, "b ... -> b (...)"), 0, repeat(indices, "z -> z d", d=second_dim)
        # ).reshape(-1, *other_shape)
        return torch.gather(
            input.view(ctx.first_axis_dim, second_dim),
            0,
            indices.unsqueeze(-1).expand(indices.size(0), second_dim)
        ).reshape(-1, *other_shape)

    @staticmethod
    def backward(ctx, grad_output):
        (indices,) = ctx.saved_tensors
        assert grad_output.ndim >= 2
        other_shape = grad_output.shape[1:]
        # grad_output = rearrange(grad_output, "b ... -> b (...)")
        grad_output = grad_output.view(grad_output.size(0), other_shape.numel())
        grad_input = torch.zeros(
            [ctx.first_axis_dim, grad_output.shape[1]],
            device=grad_output.device,
            dtype=grad_output.dtype,
        )
        # TD [2022-03-04] For some reason torch.scatter is a bit faster than indexing.
        # grad_input[indices] = grad_output
        # grad_input.scatter_(0, repeat(indices, "z -> z d", d=grad_output.shape[1]), grad_output)
        grad_input.scatter_(
            0, indices.unsqueeze(-1).expand(indices.size(0), grad_output.size(1)), grad_output
        )
        return grad_input.reshape(ctx.first_axis_dim, *other_shape), None


index_first_axis = IndexFirstAxis.apply


def unpad_input(hidden_states, attention_mask=None, indices=None):
    """
    Arguments:
        hidden_states: (batch, seqlen, ...)
        attention_mask: (batch, seqlen), bool / int, 1 means valid and 0 means not valid.
        indices: (total_nnz), the indices of non-masked tokens from the flattened input sequence.
    Return:
        hidden_states: (total_nnz, ...), where total_nnz = number of tokens in selected in attention_mask.
    """
    if indices is None:
        assert attention_mask is not None
        indices = torch.nonzero(attention_mask.flatten(), as_tuple=False).flatten()

    # TD [2022-03-04] We don't want to index with a bool mask, because Pytorch will expand the
    # bool mask, then call nonzero to get the indices, then index with those. The indices is @dim
    # times larger than it needs to be, wasting memory. It's faster and more memory-efficient to
    # index with integer indices. Moreover, torch's index is a bit slower than it needs to be,
    # so we write custom forward and backward to make it a bit faster.
    hidden_states = hidden_states.view(-1, *hidden_states.shape[2:])
    return index_first_axis(hidden_states, indices)


class IndexPutFirstAxis(torch.autograd.Function):
    @staticmethod
    def forward(
        ctx,
        values: torch.Tensor,
        indices: torch.Tensor,
        first_axis_dim
    ) -> torch.Tensor:
        ctx.save_for_backward(indices)
        assert indices.ndim == 1
        assert values.ndim >= 2
        output = torch.zeros(
            first_axis_dim, *values.shape[1:], device=values.device, dtype=values.dtype
        )
        output[indices] = values
        return output

    @staticmethod
    def backward(ctx, grad_output: torch.Tensor) -> Tuple[torch.Tensor, None, None]:
        indices, = ctx.saved_tensors
        grad_values = grad_output[indices]
        return grad_values, None, None


index_put_first_axis = IndexPutFirstAxis.apply


def pad_input(inputs: torch.Tensor, indices: torch.Tensor, batch: int, seqlen: int) -> torch.Tensor:
    """Add padding to sequences.

    Arguments:
        inputs: (total_nnz, ...), where total_nnz = number of tokens in selected in attention_mask.
        indices: (total_nnz), `indices = torch.nonzero(attention_mask.flatten(), as_tuple=False).flatten()`
        batch: int batch_size
        seqlen: int max sequence length

    Returns:
        inputs: (batch, seqlen, ...)
    """
    output = index_put_first_axis(inputs, indices, batch * seqlen)
    return output.view(batch, seqlen, *inputs.shape[1:])


def rotate_half(x):
    """Rotates half the hidden dims of the input."""
    x1 = x[..., : x.shape[-1] // 2]
    x2 = x[..., x.shape[-1] // 2 :]
    return torch.cat((-x2, x1), dim=-1)


def apply_rotary_pos_emb(q, k, cos, sin):
    """Applies Rotary Position Embedding to the query and key tensors.

    Args:
        q (`torch.Tensor`): The query tensor.
        k (`torch.Tensor`): The key tensor.
        cos (`torch.Tensor`): The cosine part of the rotary embedding.
        sin (`torch.Tensor`): The sine part of the rotary embedding.
    Returns:
        `tuple(torch.Tensor)` comprising of the query and key tensors rotated using the Rotary Position Embedding.
    """
    cos, sin = cos.to(q.dtype), sin.to(q.dtype)
    q_embed = (q * cos) + (rotate_half(q) * sin)
    k_embed = (k * cos) + (rotate_half(k) * sin)
    return q_embed, k_embed


class RotaryEmbedding(torch.nn.Module):
    def __init__(self, dim, max_position_embeddings=512, base=10000.0, device=None):
        super().__init__()

        self.dim = dim
        self.max_position_embeddings = max_position_embeddings
        self.base = base
        inv_freq = 1.0 / (self.base ** (torch.arange(0, self.dim, 2).float().to(device) / self.dim))
        self.register_buffer("inv_freq", inv_freq, persistent=False)

        # Build here to make `torch.jit.trace` work.
        self._set_cos_sin_cache(
            seq_len=max_position_embeddings, device=self.inv_freq.device, dtype=torch.get_default_dtype()
        )

    def _set_cos_sin_cache(self, seq_len, device, dtype):
        self.max_seq_len_cached = seq_len
        t = torch.arange(self.max_seq_len_cached, device=device, dtype=torch.float32)

        freqs = torch.einsum("i,j->ij", t, self.inv_freq)
        # Different from paper, but it uses a different permutation in order to obtain the same calculation
        emb = torch.cat((freqs, freqs), dim=-1)
        self.register_buffer("cos_cached", emb.cos().to(dtype), persistent=False)
        self.register_buffer("sin_cached", emb.sin().to(dtype), persistent=False)

    def forward(self, x, seq_len=None):
        # x: [bs, num_attention_heads, seq_len, head_size]
        if seq_len > self.max_seq_len_cached:
            self._set_cos_sin_cache(seq_len=seq_len, device=x.device, dtype=x.dtype)

        return (
            self.cos_cached[:seq_len, ...].to(dtype=x.dtype),
            self.sin_cached[:seq_len, ...].to(dtype=x.dtype),
        )


class NTKScalingRotaryEmbedding(RotaryEmbedding):
    """RotaryEmbedding extended with fixed and mixed NTK scaling. https://kexue.fm/archives/9706 """

    def __init__(self, dim, max_position_embeddings=512, base=10000, device=None, scaling_factor=1.0, mixed_b=None):
        self.scaling_factor = scaling_factor
        self.mixed_b = mixed_b
        super().__init__(dim, max_position_embeddings, base, device)
        max_position_embeddings = max_position_embeddings * self.scaling_factor
        self._set_cos_sin_cache(max_position_embeddings, self.inv_freq.device, torch.get_default_dtype())

    def _set_cos_sin_cache(self, seq_len, device, dtype):
        self.max_seq_len_cached = seq_len

        if seq_len > self.max_position_embeddings:
            base = self.base * (self.scaling_factor if self.mixed_b is None else 1)
            inv_freq = 1.0 / (base ** (torch.arange(0, self.dim, 2).float().to(device) / self.dim))

            if self.mixed_b is None:
                inv_freq = inv_freq / self.scaling_factor ** (2 / self.dim)  # (6)
            else:
                a = torch.tensor(self.scaling_factor).log() / (self.dim / 2) ** self.mixed_b  # (13)
                lambda_1_m = (a * torch.arange(1, self.dim // 2 + 1).float().to(device) ** self.mixed_b).exp()  # (12)
                inv_freq = inv_freq / lambda_1_m  # (10)

            self.register_buffer("inv_freq", inv_freq, persistent=False)

        t = torch.arange(self.max_seq_len_cached, device=device, dtype=torch.float32)

        freqs = torch.einsum("i,j->ij", t, self.inv_freq)
        # Different from paper, but it uses a different permutation in order to obtain the same calculation
        emb = torch.cat((freqs, freqs), dim=-1)
        self.register_buffer("cos_cached", emb.cos().to(dtype), persistent=False)
        self.register_buffer("sin_cached", emb.sin().to(dtype), persistent=False)


class RMSNorm(nn.Module):
    def __init__(self, hidden_size, eps=1e-6):
        """
        RMSNorm is equivalent to T5LayerNorm
        """
        super().__init__()
        self.weight = nn.Parameter(torch.ones(hidden_size))
        self.variance_epsilon = eps

    def forward(self, hidden_states):
        input_dtype = hidden_states.dtype
        hidden_states = hidden_states.to(torch.float32)
        variance = hidden_states.pow(2).mean(-1, keepdim=True)
        hidden_states = hidden_states * torch.rsqrt(variance + self.variance_epsilon)
        return self.weight * hidden_states.to(input_dtype)


LAYER_NORM = {
    'layer_norm': nn.LayerNorm,
    'rms_norm': RMSNorm
}


class NewEmbeddings(nn.Module):
    """
    Embedding and Unpadding.
    """

    def __init__(self, config: NewConfig):
        super().__init__()
        self.padding_idx = config.pad_token_id
        self.word_embeddings = nn.Embedding(
            config.vocab_size, config.hidden_size, padding_idx=self.padding_idx
        )

        self.position_embedding_type = config.position_embedding_type
        if self.position_embedding_type == 'absolute':
            self.position_embeddings = nn.Embedding(
                config.max_position_embeddings, config.hidden_size, padding_idx=self.padding_idx
            )
        elif self.position_embedding_type == 'rope':
            self._init_rope(config)
        else:
            raise ValueError

        self.type_vocab_size = config.type_vocab_size
        if self.type_vocab_size > 0:
            self.token_type_embeddings = nn.Embedding(config.type_vocab_size, config.hidden_size)

        # self.LayerNorm is not snake-cased to stick with TensorFlow model variable name and be able to load
        # any TensorFlow checkpoint file
        self.LayerNorm = nn.LayerNorm(config.hidden_size, eps=config.layer_norm_eps)
        self.dropout = nn.Dropout(config.hidden_dropout_prob)
        # position_ids is contiguous in memory and excluded when serialized
        self.register_buffer(
            "position_ids", torch.arange(config.max_position_embeddings), persistent=False
        )

    def _init_rope(self, config):
        kwargs = dict(
            dim=int(config.hidden_size / config.num_attention_heads),
            max_position_embeddings=config.max_position_embeddings,
            base=config.rope_theta
        )
        if config.rope_scaling is None:
            self.rotary_emb = RotaryEmbedding(**kwargs)
        else:
            kwargs.update(scaling_factor=config.rope_scaling["factor"])
            scaling_type = config.rope_scaling["type"]
            if scaling_type == 'ntk':
                kwargs.update(mixed_b=config.rope_scaling.get('mixed_b', None))
                self.rotary_emb = NTKScalingRotaryEmbedding(**kwargs)
            # elif scaling_type == "linear":
            #     self.rotary_emb = LinearScalingRotaryEmbedding(**kwargs)
            # elif scaling_type == "dynamic":
            #     self.rotary_emb = DynamicNTKScalingRotaryEmbedding(**kwargs)
            else:
                raise ValueError(f"Unknown RoPE scaling type {scaling_type}")

    def forward(
        self,
        unpad_inputs: bool,
        input_ids: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None,
        length: Optional[List[int]] = None,
        token_type_ids: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
        inputs_embeds: Optional[torch.Tensor] = None,
    ) -> Tuple[torch.Tensor, torch.Tensor, Optional[Tuple], Optional[List[int]]]:
        """
        """
        if inputs_embeds is None:
            device, input_shape = input_ids.device, input_ids.shape
        else:
            device, input_shape = inputs_embeds.device, inputs_embeds.shape[:2]
        batch_size, seq_length = input_shape

        # Set attention_mask if it's None
        if attention_mask is None:
            attention_mask = torch.ones(input_shape, device=device)
            if length is not None:
                for i, l in enumerate(length):
                    attention_mask[i, l:] = 0

        # Set attention_mask_bool for unpadding
        if unpad_inputs:
            attention_mask_bool = attention_mask.bool()
            if length is None:
                length = attention_mask.sum(-1).tolist()

        # Get word embeddings
        if inputs_embeds is None:
            if unpad_inputs:
                input_ids = input_ids[attention_mask_bool].unsqueeze(0)
            inputs_embeds = self.word_embeddings(input_ids)
        else:
            if unpad_inputs:
                inputs_embeds = inputs_embeds[attention_mask_bool].unsqueeze(0)
        embeddings = inputs_embeds

        # Set and unpad position_ids
        if position_ids is None:
            if seq_length > self.position_ids.size(0):
                self.register_buffer(
                    "position_ids", torch.arange(seq_length, device=embeddings.device), persistent=False
                )
            if unpad_inputs:
                # [1, cumsum_seq_len]
                position_ids = torch.cat([self.position_ids[:l] for l in length]).unsqueeze(0)
            else:
                # [bs, seq_len]
                position_ids = self.position_ids[:seq_length].expand(batch_size, -1)
        elif unpad_inputs:
            position_ids = position_ids[attention_mask_bool].unsqueeze(0)  # [1, cumsum_seq_len]

        # Compute rotary embedding
        if self.position_embedding_type == 'rope':
            rope_cos, rope_sin = self.rotary_emb(inputs_embeds, seq_len=seq_length)
            rope_cos = rope_cos[position_ids].unsqueeze(2)  # [bs, seq_len, 1, dim]
            rope_sin = rope_sin[position_ids].unsqueeze(2)  # [bs, seq_len, 1, dim]
            rope_embeds = rope_cos, rope_sin
        else:
            rope_embeds = None

        if self.type_vocab_size > 0:
            if token_type_ids is None:
                token_type_ids = position_ids.mul(0)
            else:
                if self.type_vocab_size < 2:
                    token_type_ids.mul_(0)
                if unpad_inputs:
                    token_type_ids = token_type_ids[attention_mask_bool].unsqueeze(0)

            token_type_embeddings = self.token_type_embeddings(token_type_ids)
            embeddings = embeddings + token_type_embeddings

        # BERT position
        if self.position_embedding_type == "absolute":
            position_embeddings = self.position_embeddings(position_ids)
            embeddings = embeddings + position_embeddings

        embeddings = self.LayerNorm(embeddings)
        embeddings = self.dropout(embeddings)

        return embeddings, attention_mask, rope_embeds, length


class NewAttention(nn.Module):
    def __init__(self, config: NewConfig, pack_qkv=None, use_memory_efficient_attention=None):
        super().__init__()
        self.config = config
        if config.hidden_size % config.num_attention_heads != 0 and not hasattr(config, "embedding_size"):
            raise ValueError(
                f"The hidden size ({config.hidden_size}) is not a multiple of the number of attention "
                f"heads ({config.num_attention_heads})"
            )

        self.hidden_size = config.hidden_size
        self.num_attention_heads = config.num_attention_heads
        self.attention_head_size = int(config.hidden_size / config.num_attention_heads)
        self.all_head_size = self.num_attention_heads * self.attention_head_size

        if pack_qkv is None:
            pack_qkv = config.pack_qkv
        self.pack_qkv = pack_qkv

        if self.pack_qkv:
            self.qkv_proj = nn.Linear(config.hidden_size, self.all_head_size * 3, bias=True)
        else:
            self.q_proj = nn.Linear(config.hidden_size, self.all_head_size, bias=True)
            self.k_proj = nn.Linear(config.hidden_size, self.all_head_size, bias=True)
            self.v_proj = nn.Linear(config.hidden_size, self.all_head_size, bias=True)

        self.dropout = nn.Dropout(config.attention_probs_dropout_prob)
        self.o_proj = nn.Linear(config.hidden_size, config.hidden_size, bias=True)

        if use_memory_efficient_attention is None:
            use_memory_efficient_attention = self.config.use_memory_efficient_attention
        self.use_memory_efficient_attention = use_memory_efficient_attention
        self.memory_efficient_attention = None if xops is None else xops.memory_efficient_attention
        if self.use_memory_efficient_attention:
            assert self.memory_efficient_attention is not None, 'please install xformers'

    def forward(
        self,
        hidden_states: torch.Tensor,
        attention_bias: torch.FloatTensor,
        rope_embeds: Optional[Tuple[torch.FloatTensor, torch.FloatTensor]] = None,
        padding_inputs: Optional[Tuple] = None,  # indices, batch, seqlen
        attention_scale: Optional[torch.FloatTensor] = None,
        head_mask: Optional[torch.FloatTensor] = None,
        output_attentions: Optional[bool] = False,
        qkv_inputs: Optional[Tuple] = None,  # For RetroMAE
    ) -> Tuple[torch.Tensor, ...]:
        shape_hd = (self.num_attention_heads, self.attention_head_size)
        # qkv
        if self.pack_qkv and qkv_inputs is None:
            qkv_pack = self.qkv_proj(hidden_states).split(self.all_head_size, dim=-1)
        else:
            if qkv_inputs is None:
                qkv_inputs = (hidden_states, hidden_states, hidden_states)
            qkv_pack = [
                getattr(self, n + '_proj')(s) for s, n in zip(qkv_inputs, 'qkv')
            ]
        query_states, key_states, value_states = [t.view(t.shape[:-1] + shape_hd) for t in qkv_pack]

        if self.config.position_embedding_type == 'rope':
            query_states, key_states = apply_rotary_pos_emb(query_states, key_states, *rope_embeds)

        dtype = query_states.dtype

        if self.config.logn_attention_scale and attention_scale is not None:
            # https://kexue.fm/archives/8823
            query_states = query_states * attention_scale.to(dtype)

        if padding_inputs is not None:
            query_states = pad_input(query_states.squeeze(), *padding_inputs)
            key_states = pad_input(key_states.squeeze(), *padding_inputs)
            value_states = pad_input(value_states.squeeze(), *padding_inputs)

        if self.use_memory_efficient_attention:
            assert self.memory_efficient_attention is not None, "xformers is not loaded"
            assert output_attentions is False, "memory_efficient_attention do not output attentions"
            assert head_mask is None, "Not support yet"
            attention_probs = None
            if torch.is_tensor(attention_bias):
                attention_bias = attention_bias.to(dtype)
            context_layer = self.memory_efficient_attention(
                query_states,
                key_states,
                value_states,
                attn_bias=attention_bias,
                p=self.dropout.p
            )
        else:
            if output_attentions and isinstance(self, NewSdpaAttention):
                raise RuntimeError("SDPA do not output attentions")
            context_layer, attention_probs = self._attention(
                query_states, key_states, value_states, attention_bias, head_mask
            )

        if padding_inputs is not None:
            context_layer = unpad_input(context_layer, indices=padding_inputs[0])

        new_context_layer_shape = context_layer.size()[:-2] + (self.all_head_size,)
        context_layer = context_layer.view(new_context_layer_shape)

        # output proj
        attn_output = self.o_proj(context_layer)

        # add attentions if we output them
        outputs = (attn_output, attention_probs) if output_attentions else (attn_output,)
        return outputs

    def _attention(self, query_states, key_states, value_states, attention_bias, head_mask):
        """
        Args:
            q/k/v: (B, L, n_head, head_dim),
        Returns:
            attn_output: (B L, n_head, head_dim)
        """
        query_states = query_states.transpose(1, 2)
        key_states = key_states.transpose(1, 2)
        value_states = value_states.transpose(1, 2)
        # Take the dot product between "query" and "key" to get the raw attention scores.
        attention_scores = torch.matmul(query_states, key_states.transpose(-1, -2))

        attention_scores = attention_scores / math.sqrt(self.attention_head_size)
        if attention_bias is not None:
            # Apply the attention mask is (precomputed for all layers in BertModel forward() function)
            attention_scores = attention_scores + attention_bias

        # Normalize the attention scores to probabilities.
        attention_probs = nn.functional.softmax(attention_scores, dim=-1)

        # This is actually dropping out entire tokens to attend to, which might
        # seem a bit unusual, but is taken from the original Transformer paper.
        if self.dropout.p > 0:
            attention_probs = self.dropout(attention_probs)

        # Mask heads if we want to
        if head_mask is not None:
            attention_probs = attention_probs * head_mask

        context_layer = torch.matmul(attention_probs, value_states)

        context_layer = context_layer.permute(0, 2, 1, 3).contiguous()
        return context_layer, attention_probs


class NewSdpaAttention(NewAttention):
    """
    New attention module using torch.nn.functional.scaled_dot_product_attention. This module inherits from
    `NewAttention` as the weights of the module stays untouched. The only changes are on the forward pass to adapt to
    SDPA API.
    """
    def __init__(self, config: NewConfig, **kwargs):
        super().__init__(config, **kwargs)
        # torch.backends.cuda.enable_mem_efficient_sdp(False)
        # logger.warning(
        #     "Disable memory efficient attention kernel for `NewSdpaAttention`, you can set "
        #     "`use_memory_efficient_attention=True` if it expected to use."
        # )

    def _attention(self, query_states, key_states, value_states, attention_bias, head_mask):
        attn_output = torch.nn.functional.scaled_dot_product_attention(
            query_states.transpose(1, 2),
            key_states.transpose(1, 2),
            value_states.transpose(1, 2),
            attn_mask=attention_bias,
            dropout_p=self.dropout.p if self.training else 0.0,
        )
        attn_output = attn_output.permute(0, 2, 1, 3).contiguous()
        return attn_output, None


NEW_ATTENTION_CLASSES = {
    "eager": NewAttention,
    # "flash_attention_2": ,  # TODO
    "sdpa": NewSdpaAttention,
}


class NewGatedMLP(nn.Module):
    """
    GLU Variants Improve Transformer.
    """

    def __init__(self, config: NewConfig):
        super().__init__()
        self.intermediate_size = config.intermediate_size
        self.up_gate_proj = nn.Linear(config.hidden_size, self.intermediate_size * 2, bias=False)
        self.down_proj = nn.Linear(self.intermediate_size, config.hidden_size, bias=True)
        self.act_fn = ACT2FN[config.hidden_act]
        if config.hidden_dropout_prob > 0:
            self.hidden_dropout = nn.Dropout(config.hidden_dropout_prob)
        else:
            self.hidden_dropout = None

    def forward(self, hidden_states):
        up_gate = self.up_gate_proj(hidden_states)
        up_states, gate = torch.split(up_gate, self.intermediate_size, dim=-1)
        gate = self.act_fn(gate)
        gated_states = gate * up_states
        if self.hidden_dropout is not None:
            gated_states = self.hidden_dropout(gated_states)
        down_states = self.down_proj(gated_states)
        return down_states


class NewLayer(nn.Module):
    def __init__(
        self,
        config: NewConfig,
        pack_qkv=None,
        use_memory_efficient_attention=None,
        attn_implementation=None
    ):
        super().__init__()
        if attn_implementation is None:
            attn_implementation = config._attn_implementation
        if use_memory_efficient_attention is None:
            use_memory_efficient_attention = config.use_memory_efficient_attention
        if use_memory_efficient_attention:
            if attn_implementation != 'eager':
                logger.warning_once(f"Override {attn_implementation=} to 'eager' as {use_memory_efficient_attention=}")
                attn_implementation = 'eager'  # Since it will be SDPA by default for torch>=2.1.1
        self.attention = NEW_ATTENTION_CLASSES[attn_implementation](
            config, pack_qkv=pack_qkv, use_memory_efficient_attention=use_memory_efficient_attention
        )
        self.mlp = NewGatedMLP(config)

        ln_class = LAYER_NORM[config.layer_norm_type]
        self.attn_ln = ln_class(config.hidden_size, eps=config.layer_norm_eps)
        self.mlp_ln = ln_class(config.hidden_size, eps=config.layer_norm_eps)

        if config.hidden_dropout_prob > 0:
            self.hidden_dropout = nn.Dropout(config.hidden_dropout_prob)
        else:
            self.hidden_dropout = None

    def forward(
        self,
        hidden_states: torch.Tensor,
        attention_bias: torch.FloatTensor,
        rope_embeds: Optional[Tuple[torch.FloatTensor, torch.FloatTensor]] = None,
        padding_inputs: Optional[Tuple] = None,  # indices, batch, seqlen
        attention_scale: Optional[torch.FloatTensor] = None,
        subset_indices: Optional[torch.LongTensor] = None,
        head_mask: Optional[torch.FloatTensor] = None,
        output_attentions: Optional[bool] = False,
        qkv_inputs: Optional[Tuple] = None,  # For RetroMAE
    ) -> Tuple[torch.Tensor, ...]:
        # Multi head self attention
        residual = hidden_states if qkv_inputs is None else qkv_inputs[0]
        attention_outputs = self.attention(
            hidden_states,
            attention_bias,
            rope_embeds,
            padding_inputs,
            attention_scale,
            head_mask,
            output_attentions=output_attentions,
            qkv_inputs=qkv_inputs,
        )
        hidden_states = attention_outputs[0]
        if self.hidden_dropout is not None:
            hidden_states = self.hidden_dropout(hidden_states)
        hidden_states = residual + hidden_states

        # In pretraining, after the attention of last layer, we only need the masked tokens.
        if subset_indices is not None:
            hidden_states = hidden_states[subset_indices]

        hidden_states = self.attn_ln(hidden_states)

        # Fully Connected
        residual = hidden_states
        hidden_states = self.mlp(hidden_states)
        if self.hidden_dropout is not None:
            hidden_states = self.hidden_dropout(hidden_states)
        hidden_states = residual + hidden_states
        hidden_states = self.mlp_ln(hidden_states)

        # add self attentions if we output attention weights
        outputs = (hidden_states,) + attention_outputs[1:]
        return outputs


class NewEncoder(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.config = config
        self.layer = nn.ModuleList([NewLayer(config) for _ in range(config.num_hidden_layers)])
        self.gradient_checkpointing = False

    def forward(
        self,
        hidden_states: torch.Tensor,
        attention_bias: Optional[torch.FloatTensor] = None,
        rope_embeds: Optional[Tuple[torch.FloatTensor, torch.FloatTensor]] = None,
        padding_inputs: Optional[Tuple] = None,  # indices, batch, seqlen
        attention_scale: Optional[torch.FloatTensor] = None,
        subset_indices: Optional[torch.LongTensor] = None,
        head_mask: Optional[torch.FloatTensor] = None,
        output_attentions: Optional[bool] = False,
        output_hidden_states: Optional[bool] = False,
        return_dict: Optional[bool] = True,
    ) -> Union[Tuple[torch.Tensor], BaseModelOutput]:
        all_hidden_states = () if output_hidden_states else None
        all_self_attentions = () if output_attentions else None

        for i, layer_module in enumerate(self.layer):
            if output_hidden_states:
                all_hidden_states = all_hidden_states + (hidden_states,)

            if i >= len(self.layer) - 1:
                layer_subset_indices = subset_indices
            else:
                layer_subset_indices = None

            layer_head_mask = head_mask[i] if head_mask is not None else None

            if self.gradient_checkpointing and self.training:
                layer_outputs = self._gradient_checkpointing_func(
                    layer_module.__call__,
                    hidden_states,
                    attention_bias,
                    rope_embeds,
                    padding_inputs,
                    attention_scale,
                    layer_subset_indices,
                    layer_head_mask,
                )
            else:
                layer_outputs = layer_module(
                    hidden_states,
                    attention_bias,
                    rope_embeds,
                    padding_inputs,
                    attention_scale,
                    layer_subset_indices,
                    layer_head_mask,
                    output_attentions,
                )

            hidden_states = layer_outputs[0]
            if output_attentions:
                all_self_attentions = all_self_attentions + (layer_outputs[1],)

        if output_hidden_states:
            all_hidden_states = all_hidden_states + (hidden_states,)

        if not return_dict:
            return tuple(
                v
                for v in [
                    hidden_states,
                    all_hidden_states,
                    all_self_attentions,
                ]
                if v is not None
            )
        return BaseModelOutput(
            last_hidden_state=hidden_states,
            hidden_states=all_hidden_states,
            attentions=all_self_attentions,
        )


# Copied from transformers.models.bert.modeling_bert.BertPooler with Bert->New
class NewPooler(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.dense = nn.Linear(config.hidden_size, config.hidden_size)
        self.activation = nn.Tanh()

    def forward(self, hidden_states: torch.Tensor) -> torch.Tensor:
        # We "pool" the model by simply taking the hidden state corresponding
        # to the first token.
        first_token_tensor = hidden_states[:, 0]
        pooled_output = self.dense(first_token_tensor)
        pooled_output = self.activation(pooled_output)
        return pooled_output


class NewPreTrainedModel(PreTrainedModel):
    """
    An abstract class to handle weights initialization and a simple interface for downloading and loading pretrained
    models.
    """

    config_class = NewConfig
    base_model_prefix = "new"
    supports_gradient_checkpointing = True
    _supports_sdpa = True

    def _init_weights(self, module):
        """Initialize the weights"""
        if isinstance(module, nn.Linear):
            # Slightly different from the TF version which uses truncated_normal for initialization
            # cf https://github.com/pytorch/pytorch/pull/5617
            module.weight.data.normal_(mean=0.0, std=self.config.initializer_range)
            if module.bias is not None:
                module.bias.data.zero_()
        elif isinstance(module, nn.Embedding):
            module.weight.data.normal_(mean=0.0, std=self.config.initializer_range)
            if module.padding_idx is not None:
                module.weight.data[module.padding_idx].zero_()
        elif isinstance(module, nn.LayerNorm):
            module.bias.data.zero_()
            module.weight.data.fill_(1.0)


class NewModel(NewPreTrainedModel):
    """
    The bare New Model transformer outputting raw hidden-states without any specific head on top.
    """

    def __init__(self, config: NewConfig, add_pooling_layer=False):
        super().__init__(config)
        self.config = config

        self.embeddings = NewEmbeddings(config)
        self.encoder = NewEncoder(config)

        self.pooler = NewPooler(config) if add_pooling_layer else None

        # Initialize weights and apply final processing
        self.post_init()

    def get_input_embeddings(self):
        return self.embeddings.word_embeddings

    def set_input_embeddings(self, value):
        self.embeddings.word_embeddings = value

    def forward(
        self,
        input_ids: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None,
        length: Optional[List[int]] = None,
        subset_indices: Optional[torch.LongTensor] = None,
        token_type_ids: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
        head_mask: Optional[torch.Tensor] = None,
        inputs_embeds: Optional[torch.Tensor] = None,
        output_attentions: Optional[bool] = None,
        output_hidden_states: Optional[bool] = None,
        return_dict: Optional[bool] = None,
        unpad_inputs: Optional[bool] = None,
    ) -> Union[Tuple[torch.Tensor], BaseModelOutputWithPooling]:
        r"""
        length  (`list` of length `batch_size`, *optional*):
            If is `None`, return padded `last_hidden_state`.
        subset_indices  ():
            pass
        unpad_inputs  (`bool`, *optional*):
            pass
        """
        output_attentions = output_attentions if output_attentions is not None else self.config.output_attentions
        output_hidden_states = (
            output_hidden_states if output_hidden_states is not None else self.config.output_hidden_states
        )
        return_dict = return_dict if return_dict is not None else self.config.use_return_dict
        unpad_inputs = unpad_inputs if unpad_inputs is not None else self.config.unpad_inputs
        output_padded = length is None

        if input_ids is not None and inputs_embeds is not None:
            raise ValueError("You cannot specify both input_ids and inputs_embeds at the same time")
        elif input_ids is not None:
            self.warn_if_padding_and_no_attention_mask(input_ids, attention_mask)
            input_shape = input_ids.size()
        elif inputs_embeds is not None:
            input_shape = inputs_embeds.size()[:-1]
        else:
            raise ValueError("You have to specify either input_ids or inputs_embeds")

        # TODO: not used
        # # Prepare head mask if needed
        # # 1.0 in head_mask indicate we keep the head
        # # attention_probs has shape bsz x n_heads x N x N
        # # input head_mask has shape [num_heads] or [num_hidden_layers x num_heads]
        # # and head_mask is converted to shape [num_hidden_layers x batch x num_heads x seq_length x seq_length]
        # head_mask = self.get_head_mask(head_mask, self.config.num_hidden_layers)

        # Get embeddings, may unpad them
        (embedding_output, attention_mask, rope_embeds, length) = self.embeddings(
            unpad_inputs,
            input_ids=input_ids,
            attention_mask=attention_mask,
            length=length,
            token_type_ids=token_type_ids,
            position_ids=position_ids,
            inputs_embeds=inputs_embeds
        )

        batch_size, seq_length = input_shape
        if unpad_inputs and self.config.use_memory_efficient_attention:
            attention_bias = xops.fmha.attn_bias.BlockDiagonalMask.from_seqlens(length)
        else:
            # We can provide a self-attention mask of dimensions [batch_size, from_seq_length, to_seq_length]
            # ourselves in which case we just need to make it broadcastable to all heads.
            attention_bias = self.get_extended_attention_mask(attention_mask, input_shape)
            if self.config.use_memory_efficient_attention:
                # Invalid shape for attention bias: torch.Size([48, 1, 1, 512]) (expected (48, 12, 512, 512))
                attention_bias = attention_bias.expand(-1, self.config.num_attention_heads, seq_length, -1)

        padding_inputs = None
        if unpad_inputs and (output_padded or not self.config.use_memory_efficient_attention):
            indices = torch.nonzero(attention_mask.flatten(), as_tuple=False).flatten()
            if not self.config.use_memory_efficient_attention:
                padding_inputs = (indices, *input_shape)

        attention_scale = None
        if self.config.logn_attention_scale:
            logger.warning_once("TODO: logn_attention_scale")
        #     # attention scale log_512(input_len)
        #     attention_scale = attention_mask.sum(1).log() / torch.tensor(self.config.max_position_embeddings).log()
        #     # inference-time logn scale need clip 1
        #     if self.config.logn_attention_clip1:
        #         attention_scale.clip_(1)
        #     attention_scale = attention_scale[:, None, None, None]
        # else:
        #     attention_scale = None

        encoder_outputs = self.encoder(
            embedding_output,
            attention_bias=attention_bias,
            rope_embeds=rope_embeds,
            padding_inputs=padding_inputs,
            attention_scale=attention_scale,
            subset_indices=subset_indices,
            head_mask=head_mask,
            output_attentions=output_attentions,
            output_hidden_states=output_hidden_states,
            return_dict=return_dict,
        )
        sequence_output = encoder_outputs[0]
        if unpad_inputs and output_padded:
            sequence_output = pad_input(
                sequence_output.squeeze(), indices, batch_size, seq_length
            )

        pooled_output = self.pooler(sequence_output) if self.pooler is not None else None

        if not return_dict:
            return (sequence_output, pooled_output) + encoder_outputs[1:]

        return BaseModelOutputWithPooling(
            last_hidden_state=sequence_output,
            pooler_output=pooled_output,
            hidden_states=encoder_outputs.hidden_states,
            attentions=encoder_outputs.attentions,
        )


class NewLMPredictionHead(nn.Module):
    def __init__(self, config):
        super().__init__()
        self.dense = nn.Linear(config.hidden_size, config.hidden_size)
        self.transform_act_fn = ACT2FN[config.hidden_act]
        self.norm = nn.LayerNorm(config.hidden_size, eps=config.layer_norm_eps)

        # The output weights are the same as the input embeddings, but there is
        # an output-only bias for each token.
        self.decoder = nn.Linear(config.hidden_size, config.vocab_size)

    def forward(self, hidden_states):
        hidden_states = self.dense(hidden_states)
        hidden_states = self.transform_act_fn(hidden_states)
        hidden_states = self.norm(hidden_states)
        hidden_states = self.decoder(hidden_states)
        return hidden_states


class NewForMaskedLM(NewPreTrainedModel):
    _tied_weights_keys = ["lm_head.decoder.bias", "lm_head.decoder.weight"]

    def __init__(self, config: NewConfig):
        super().__init__(config)
        self.new = NewModel(config, add_pooling_layer=False)
        self.lm_head = NewLMPredictionHead(config)
        self.loss_fct = nn.CrossEntropyLoss()

        # Initialize weights and apply final processing
        self.post_init()

    def get_output_embeddings(self):
        return self.lm_head.decoder

    def set_output_embeddings(self, new_embeddings):
        self.lm_head.decoder = new_embeddings

    def forward(
        self,
        input_ids: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None,
        token_type_ids: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
        head_mask: Optional[torch.Tensor] = None,
        inputs_embeds: Optional[torch.Tensor] = None,
        labels: Optional[torch.Tensor] = None,
        output_attentions: Optional[bool] = None,
        output_hidden_states: Optional[bool] = None,
        return_dict: Optional[bool] = None,
        unpad_inputs: Optional[bool] = None,
    ) -> Union[Tuple[torch.Tensor], MaskedLMOutput]:
        r"""
        labels (`torch.LongTensor` of shape `(batch_size, sequence_length)`, *optional*):
            Labels for computing the masked language modeling loss. Indices should be in `[-100, 0, ...,
            config.vocab_size]` (see `input_ids` docstring) Tokens with indices set to `-100` are ignored (masked), the
            loss is only computed for the tokens with labels in `[0, ..., config.vocab_size]`
        """

        return_dict = return_dict if return_dict is not None else self.config.use_return_dict

        if labels is None or not self.new.config.unpad_inputs:
            length = None
            subset_indices = None
        else:
            length = attention_mask.sum(-1).tolist()
            labels = labels[attention_mask.bool()].unsqueeze(0)
            subset_indices = labels > -100

        outputs = self.new(
            input_ids,
            attention_mask=attention_mask,
            length=length,
            subset_indices=subset_indices,
            token_type_ids=token_type_ids,
            position_ids=position_ids,
            head_mask=head_mask,
            inputs_embeds=inputs_embeds,
            output_attentions=output_attentions,
            output_hidden_states=output_hidden_states,
            return_dict=return_dict,
            unpad_inputs=unpad_inputs,
        )

        sequence_output = outputs[0]
        prediction_scores = self.lm_head(sequence_output)

        masked_lm_loss = None
        if labels is not None:
            if subset_indices is None:
                mask = attention_mask.bool()
                prediction_scores = prediction_scores[mask]
                labels = labels[mask]
            else:
                labels = labels[subset_indices]
            masked_lm_loss = self.loss_fct(prediction_scores, labels)

        if not return_dict:
            output = (prediction_scores,) + outputs[2:]
            return ((masked_lm_loss,) + output) if masked_lm_loss is not None else output

        return MaskedLMOutput(
            loss=masked_lm_loss,
            logits=prediction_scores,
            hidden_states=outputs.hidden_states,
            attentions=outputs.attentions,
        )


class NewForSequenceClassification(NewPreTrainedModel):
    def __init__(self, config):
        super().__init__(config)
        self.num_labels = config.num_labels
        self.config = config

        self.new = NewModel(config, add_pooling_layer=True)
        classifier_dropout = (
            config.classifier_dropout if config.classifier_dropout is not None else config.hidden_dropout_prob
        )
        self.dropout = nn.Dropout(classifier_dropout)
        self.classifier = nn.Linear(config.hidden_size, config.num_labels)

        # Initialize weights and apply final processing
        self.post_init()

    def forward(
        self,
        input_ids: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None,
        token_type_ids: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
        head_mask: Optional[torch.Tensor] = None,
        inputs_embeds: Optional[torch.Tensor] = None,
        labels: Optional[torch.Tensor] = None,
        output_attentions: Optional[bool] = None,
        output_hidden_states: Optional[bool] = None,
        return_dict: Optional[bool] = None,
        unpad_inputs: Optional[bool] = None,
    ) -> Union[Tuple[torch.Tensor], SequenceClassifierOutput]:
        r"""
        labels (`torch.LongTensor` of shape `(batch_size,)`, *optional*):
            Labels for computing the sequence classification/regression loss. Indices should be in `[0, ...,
            config.num_labels - 1]`. If `config.num_labels == 1` a regression loss is computed (Mean-Square loss), If
            `config.num_labels > 1` a classification loss is computed (Cross-Entropy).
        """
        return_dict = return_dict if return_dict is not None else self.config.use_return_dict

        outputs = self.new(
            input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
            position_ids=position_ids,
            head_mask=head_mask,
            inputs_embeds=inputs_embeds,
            output_attentions=output_attentions,
            output_hidden_states=output_hidden_states,
            return_dict=return_dict,
            unpad_inputs=unpad_inputs,
        )

        pooled_output = outputs[1]

        pooled_output = self.dropout(pooled_output)
        logits = self.classifier(pooled_output)

        loss = None
        if labels is not None:
            if self.config.problem_type is None:
                if self.num_labels == 1:
                    self.config.problem_type = "regression"
                elif self.num_labels > 1 and (labels.dtype == torch.long or labels.dtype == torch.int):
                    self.config.problem_type = "single_label_classification"
                else:
                    self.config.problem_type = "multi_label_classification"

            if self.config.problem_type == "regression":
                loss_fct = nn.MSELoss()
                if self.num_labels == 1:
                    loss = loss_fct(logits.squeeze(), labels.squeeze())
                else:
                    loss = loss_fct(logits, labels)
            elif self.config.problem_type == "single_label_classification":
                loss_fct = nn.CrossEntropyLoss()
                loss = loss_fct(logits.view(-1, self.num_labels), labels.view(-1))
            elif self.config.problem_type == "multi_label_classification":
                loss_fct = nn.BCEWithLogitsLoss()
                loss = loss_fct(logits, labels)

        if not return_dict:
            output = (logits,) + outputs[2:]
            return ((loss,) + output) if loss is not None else output

        return SequenceClassifierOutput(
            loss=loss,
            logits=logits,
            hidden_states=outputs.hidden_states,
            attentions=outputs.attentions,
        )


class NewForMultipleChoice(NewPreTrainedModel):
    def __init__(self, config):
        super().__init__(config)

        self.new = NewModel(config, add_pooling_layer=True)
        classifier_dropout = (
            config.classifier_dropout if config.classifier_dropout is not None else config.hidden_dropout_prob
        )
        self.dropout = nn.Dropout(classifier_dropout)
        self.classifier = nn.Linear(config.hidden_size, 1)

        # Initialize weights and apply final processing
        self.post_init()

    def forward(
        self,
        input_ids: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None,
        token_type_ids: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
        head_mask: Optional[torch.Tensor] = None,
        inputs_embeds: Optional[torch.Tensor] = None,
        labels: Optional[torch.Tensor] = None,
        output_attentions: Optional[bool] = None,
        output_hidden_states: Optional[bool] = None,
        return_dict: Optional[bool] = None,
        unpad_inputs: Optional[bool] = None,
    ) -> Union[Tuple[torch.Tensor], MultipleChoiceModelOutput]:
        r"""
        labels (`torch.LongTensor` of shape `(batch_size,)`, *optional*):
            Labels for computing the multiple choice classification loss. Indices should be in `[0, ...,
            num_choices-1]` where `num_choices` is the size of the second dimension of the input tensors. (See
            `input_ids` above)
        """
        return_dict = return_dict if return_dict is not None else self.config.use_return_dict
        num_choices = input_ids.shape[1] if input_ids is not None else inputs_embeds.shape[1]

        input_ids = input_ids.view(-1, input_ids.size(-1)) if input_ids is not None else None
        attention_mask = attention_mask.view(-1, attention_mask.size(-1)) if attention_mask is not None else None
        token_type_ids = token_type_ids.view(-1, token_type_ids.size(-1)) if token_type_ids is not None else None
        position_ids = position_ids.view(-1, position_ids.size(-1)) if position_ids is not None else None
        inputs_embeds = (
            inputs_embeds.view(-1, inputs_embeds.size(-2), inputs_embeds.size(-1))
            if inputs_embeds is not None
            else None
        )

        outputs = self.new(
            input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
            position_ids=position_ids,
            head_mask=head_mask,
            inputs_embeds=inputs_embeds,
            output_attentions=output_attentions,
            output_hidden_states=output_hidden_states,
            return_dict=return_dict,
            unpad_inputs=unpad_inputs,
        )

        pooled_output = outputs[1]

        pooled_output = self.dropout(pooled_output)
        logits = self.classifier(pooled_output)
        reshaped_logits = logits.view(-1, num_choices)

        loss = None
        if labels is not None:
            loss_fct = nn.CrossEntropyLoss()
            loss = loss_fct(reshaped_logits, labels)

        if not return_dict:
            output = (reshaped_logits,) + outputs[2:]
            return ((loss,) + output) if loss is not None else output

        return MultipleChoiceModelOutput(
            loss=loss,
            logits=reshaped_logits,
            hidden_states=outputs.hidden_states,
            attentions=outputs.attentions,
        )


@dataclass
class NewTokenClassifierOutput(ModelOutput):
    loss: Optional[torch.FloatTensor] = None
    logits: torch.FloatTensor = None
    last_hidden_state: torch.FloatTensor = None
    hidden_states: Optional[Tuple[torch.FloatTensor, ...]] = None
    attentions: Optional[Tuple[torch.FloatTensor, ...]] = None


class NewForTokenClassification(NewPreTrainedModel):
    def __init__(self, config):
        super().__init__(config)
        self.num_labels = config.num_labels

        self.new = NewModel(config, add_pooling_layer=False)
        classifier_dropout = (
            config.classifier_dropout if config.classifier_dropout is not None else config.hidden_dropout_prob
        )
        self.dropout = nn.Dropout(classifier_dropout)
        self.classifier = nn.Linear(config.hidden_size, config.num_labels)

        # Initialize weights and apply final processing
        self.post_init()

    def forward(
        self,
        input_ids: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None,
        token_type_ids: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
        head_mask: Optional[torch.Tensor] = None,
        inputs_embeds: Optional[torch.Tensor] = None,
        labels: Optional[torch.Tensor] = None,
        output_attentions: Optional[bool] = None,
        output_hidden_states: Optional[bool] = None,
        return_dict: Optional[bool] = None,
        unpad_inputs: Optional[bool] = None,
    ) -> Union[Tuple[torch.Tensor], NewTokenClassifierOutput]:
        r"""
        labels (`torch.LongTensor` of shape `(batch_size, sequence_length)`, *optional*):
            Labels for computing the token classification loss. Indices should be in `[0, ..., config.num_labels - 1]`.
        """
        return_dict = return_dict if return_dict is not None else self.config.use_return_dict

        outputs = self.new(
            input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
            position_ids=position_ids,
            head_mask=head_mask,
            inputs_embeds=inputs_embeds,
            output_attentions=output_attentions,
            output_hidden_states=output_hidden_states,
            return_dict=return_dict,
            unpad_inputs=unpad_inputs,
        )

        sequence_output = outputs[0]

        sequence_output = self.dropout(sequence_output)
        logits = self.classifier(sequence_output)

        loss = None
        if labels is not None:
            loss_fct = nn.CrossEntropyLoss()
            loss = loss_fct(logits.view(-1, self.num_labels), labels.view(-1))

        if not return_dict:
            output = (logits,) + outputs[2:]
            return ((loss,) + output) if loss is not None else output

        return NewTokenClassifierOutput(
            loss=loss,
            logits=logits,
            last_hidden_state=sequence_output,
            hidden_states=outputs.hidden_states,
            attentions=outputs.attentions,
        )


class NewForQuestionAnswering(NewPreTrainedModel):
    def __init__(self, config):
        super().__init__(config)
        self.num_labels = config.num_labels

        self.new = NewModel(config, add_pooling_layer=False)
        self.qa_outputs = nn.Linear(config.hidden_size, config.num_labels)

        # Initialize weights and apply final processing
        self.post_init()

    def forward(
        self,
        input_ids: Optional[torch.Tensor] = None,
        attention_mask: Optional[torch.Tensor] = None,
        token_type_ids: Optional[torch.Tensor] = None,
        position_ids: Optional[torch.Tensor] = None,
        head_mask: Optional[torch.Tensor] = None,
        inputs_embeds: Optional[torch.Tensor] = None,
        start_positions: Optional[torch.Tensor] = None,
        end_positions: Optional[torch.Tensor] = None,
        output_attentions: Optional[bool] = None,
        output_hidden_states: Optional[bool] = None,
        return_dict: Optional[bool] = None,
        unpad_inputs: Optional[bool] = None,
    ) -> Union[Tuple[torch.Tensor], QuestionAnsweringModelOutput]:
        r"""
        start_positions (`torch.LongTensor` of shape `(batch_size,)`, *optional*):
            Labels for position (index) of the start of the labelled span for computing the token classification loss.
            Positions are clamped to the length of the sequence (`sequence_length`). Position outside of the sequence
            are not taken into account for computing the loss.
        end_positions (`torch.LongTensor` of shape `(batch_size,)`, *optional*):
            Labels for position (index) of the end of the labelled span for computing the token classification loss.
            Positions are clamped to the length of the sequence (`sequence_length`). Position outside of the sequence
            are not taken into account for computing the loss.
        """
        return_dict = return_dict if return_dict is not None else self.config.use_return_dict

        outputs = self.new(
            input_ids,
            attention_mask=attention_mask,
            token_type_ids=token_type_ids,
            position_ids=position_ids,
            head_mask=head_mask,
            inputs_embeds=inputs_embeds,
            output_attentions=output_attentions,
            output_hidden_states=output_hidden_states,
            return_dict=return_dict,
            unpad_inputs=unpad_inputs,
        )

        sequence_output = outputs[0]

        logits = self.qa_outputs(sequence_output)
        start_logits, end_logits = logits.split(1, dim=-1)
        start_logits = start_logits.squeeze(-1).contiguous()
        end_logits = end_logits.squeeze(-1).contiguous()

        total_loss = None
        if start_positions is not None and end_positions is not None:
            # If we are on multi-GPU, split add a dimension
            if len(start_positions.size()) > 1:
                start_positions = start_positions.squeeze(-1)
            if len(end_positions.size()) > 1:
                end_positions = end_positions.squeeze(-1)
            # sometimes the start/end positions are outside our model inputs, we ignore these terms
            ignored_index = start_logits.size(1)
            start_positions = start_positions.clamp(0, ignored_index)
            end_positions = end_positions.clamp(0, ignored_index)

            loss_fct = nn.CrossEntropyLoss(ignore_index=ignored_index)
            start_loss = loss_fct(start_logits, start_positions)
            end_loss = loss_fct(end_logits, end_positions)
            total_loss = (start_loss + end_loss) / 2

        if not return_dict:
            output = (start_logits, end_logits) + outputs[2:]
            return ((total_loss,) + output) if total_loss is not None else output

        return QuestionAnsweringModelOutput(
            loss=total_loss,
            start_logits=start_logits,
            end_logits=end_logits,
            hidden_states=outputs.hidden_states,
            attentions=outputs.attentions,
        )

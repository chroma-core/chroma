"""
Much of this file is copied from https://github.com/sionic-ai/muvera-py/blob/master/fde_generator.py
licensed under the Apache 2.0 license.

 TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

   APPENDIX: How to apply the Apache License to your work.

      To apply the Apache License to your work, attach the following
      boilerplate notice, with the fields enclosed by brackets "[]"
      replaced with your own identifying information. (Don't include
      the brackets!)  The text should be enclosed in the appropriate
      comment syntax for the file format. We also recommend that a
      file or class name and description of purpose be included on the
      same "printed page" as the copyright notice for easier
      identification within third-party archives.

   Copyright [yyyy] [name of copyright owner]

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
from dataclasses import dataclass, replace
from enum import Enum
from typing import List, Optional, Sequence

import numpy as np

from chromadb.api.types import Embedding, Embeddings


class EncodingType(Enum):
    DEFAULT_SUM = 0
    AVERAGE = 1


class ProjectionType(Enum):
    DEFAULT_IDENTITY = 0
    AMS_SKETCH = 1


@dataclass
class FixedDimensionalEncodingConfig:
    dimension: int = 128
    num_repetitions: int = 10
    num_simhash_projections: int = 6
    seed: int = 42
    encoding_type: EncodingType = EncodingType.DEFAULT_SUM
    projection_type: ProjectionType = ProjectionType.DEFAULT_IDENTITY
    projection_dimension: Optional[int] = None
    fill_empty_partitions: bool = False
    final_projection_dimension: Optional[int] = None


def _append_to_gray_code(gray_code: int, bit: bool) -> int:
    return (gray_code << 1) + (int(bit) ^ (gray_code & 1))


def _gray_code_to_binary(num: int) -> int:
    mask = num >> 1
    while mask != 0:
        num = num ^ mask
        mask >>= 1
    return num


def _simhash_matrix_from_seed(
    dimension: int, num_projections: int, seed: int
) -> Embedding:
    rng = np.random.default_rng(seed)
    return rng.normal(loc=0.0, scale=1.0, size=(dimension, num_projections)).astype(
        np.float32
    )


def _ams_projection_matrix_from_seed(
    dimension: int, projection_dim: int, seed: int
) -> Embedding:
    rng = np.random.default_rng(seed)
    out = np.zeros((dimension, projection_dim), dtype=np.float32)
    indices = rng.integers(0, projection_dim, size=dimension)
    signs = rng.choice([-1.0, 1.0], size=dimension)
    out[np.arange(dimension), indices] = signs
    return out


def _apply_count_sketch_to_vector(
    input_vector: Embedding, final_dimension: int, seed: int
) -> Embedding:
    rng = np.random.default_rng(seed)
    out = np.zeros(final_dimension, dtype=np.float32)
    indices = rng.integers(0, final_dimension, size=input_vector.shape[0])
    signs = rng.choice([-1.0, 1.0], size=input_vector.shape[0])
    np.add.at(out, indices, signs * input_vector)
    return out


def _simhash_partition_index_gray(sketch_vector: Embedding) -> int:
    partition_index = 0
    for val in sketch_vector:
        partition_index = _append_to_gray_code(partition_index, val > 0)
    return partition_index


def _distance_to_simhash_partition(
    sketch_vector: Embedding, partition_index: int
) -> int:
    num_projections = sketch_vector.size
    binary_representation = _gray_code_to_binary(partition_index)
    sketch_bits = (sketch_vector > 0).astype(int)
    binary_array = (binary_representation >> np.arange(num_projections - 1, -1, -1)) & 1
    return int(np.sum(sketch_bits != binary_array))


def _generate_fde_internal(
    point_cloud: Embedding, config: FixedDimensionalEncodingConfig
) -> Embedding:
    if point_cloud.ndim != 2 or point_cloud.shape[1] != config.dimension:
        raise ValueError(
            f"Input data shape {point_cloud.shape} is inconsistent with config dimension {config.dimension}."
        )
    if not (0 <= config.num_simhash_projections < 32):
        raise ValueError(
            f"num_simhash_projections must be in [0, 31]: {config.num_simhash_projections}"
        )

    num_points, original_dim = point_cloud.shape
    num_partitions = 2**config.num_simhash_projections

    use_identity_proj = config.projection_type == ProjectionType.DEFAULT_IDENTITY
    projection_dim = original_dim if use_identity_proj else config.projection_dimension
    if not use_identity_proj and (not projection_dim or projection_dim <= 0):
        raise ValueError(
            "A positive projection_dimension is required for non-identity projections."
        )

    final_fde_dim = config.num_repetitions * num_partitions * projection_dim
    out_fde = np.zeros(final_fde_dim, dtype=np.float32)

    for rep_num in range(config.num_repetitions):
        current_seed = config.seed + rep_num

        sketches = np.dot(
            point_cloud,
            _simhash_matrix_from_seed(
                original_dim, config.num_simhash_projections, current_seed
            ),
        )

        if use_identity_proj:
            projected_matrix = point_cloud
        elif config.projection_type == ProjectionType.AMS_SKETCH:
            ams_matrix = _ams_projection_matrix_from_seed(
                original_dim, projection_dim, current_seed
            )
            projected_matrix = np.dot(point_cloud, ams_matrix)

        rep_fde_sum = np.zeros(num_partitions * projection_dim, dtype=np.float32)
        partition_counts = np.zeros(num_partitions, dtype=np.int32)
        partition_indices = np.array(
            [_simhash_partition_index_gray(sketches[i]) for i in range(num_points)]
        )

        for i in range(num_points):
            start_idx = partition_indices[i] * projection_dim
            rep_fde_sum[start_idx : start_idx + projection_dim] += projected_matrix[i]
            partition_counts[partition_indices[i]] += 1

        if config.encoding_type == EncodingType.AVERAGE:
            for i in range(num_partitions):
                start_idx = i * projection_dim
                if partition_counts[i] > 0:
                    rep_fde_sum[
                        start_idx : start_idx + projection_dim
                    ] /= partition_counts[i]
                elif config.fill_empty_partitions and num_points > 0:
                    distances = [
                        _distance_to_simhash_partition(sketches[j], i)
                        for j in range(num_points)
                    ]
                    nearest_point_idx = np.argmin(distances)
                    rep_fde_sum[
                        start_idx : start_idx + projection_dim
                    ] = projected_matrix[nearest_point_idx]

        rep_start_index = rep_num * num_partitions * projection_dim
        out_fde[rep_start_index : rep_start_index + rep_fde_sum.size] = rep_fde_sum

    if config.final_projection_dimension and config.final_projection_dimension > 0:
        return _apply_count_sketch_to_vector(
            out_fde, config.final_projection_dimension, config.seed
        )

    return out_fde


def generate_query_fde(
    point_cloud: Embedding, config: FixedDimensionalEncodingConfig
) -> Embedding:
    """Generates a Fixed Dimensional Encoding for a query point cloud (using SUM)."""
    if config.fill_empty_partitions:
        raise ValueError(
            "Query FDE generation does not support 'fill_empty_partitions'."
        )
    query_config = replace(config, encoding_type=EncodingType.DEFAULT_SUM)
    return _generate_fde_internal(point_cloud, query_config)


def generate_document_fde(
    point_cloud: Embedding, config: FixedDimensionalEncodingConfig
) -> Embedding:
    """Generates a Fixed Dimensional Encoding for a document point cloud (using AVERAGE)."""
    doc_config = replace(config, encoding_type=EncodingType.AVERAGE)
    return _generate_fde_internal(point_cloud, doc_config)


def _build_fde_config(
    *,
    dims: int,
    k_sim: int,
    proj_dim: Optional[int],
    num_repetitions: int,
    seed: int,
    fill_empty_partitions: bool,
    final_dim: Optional[int],
    encoding_type: EncodingType,
) -> FixedDimensionalEncodingConfig:
    projection_type = (
        ProjectionType.DEFAULT_IDENTITY
        if proj_dim is None or proj_dim == dims
        else ProjectionType.AMS_SKETCH
    )

    projection_dimension = (
        None if projection_type == ProjectionType.DEFAULT_IDENTITY else proj_dim
    )

    return FixedDimensionalEncodingConfig(
        dimension=dims,
        num_repetitions=num_repetitions,
        num_simhash_projections=k_sim,
        seed=seed,
        encoding_type=encoding_type,
        projection_type=projection_type,
        projection_dimension=projection_dimension,
        fill_empty_partitions=fill_empty_partitions,
        final_projection_dimension=final_dim,
    )


def generate_document_fde_batch(
    doc_embeddings_list: Sequence[Embeddings], config: FixedDimensionalEncodingConfig
) -> List[Embedding]:
    """
    Generates FDEs for a batch of documents using highly optimized NumPy vectorization.
    Fully compliant with C++ implementation including all projection types.
    """
    num_docs = len(doc_embeddings_list)

    if num_docs == 0:
        return np.array([])

    # Input validation
    valid_docs = []
    for i, doc in enumerate(doc_embeddings_list):
        if doc.ndim != 2:
            continue
        if doc.shape[1] != config.dimension:
            raise ValueError(
                f"Document {i} has incorrect dimension: expected {config.dimension}, got {doc.shape[1]}"
            )
        if doc.shape[0] == 0:
            continue
        valid_docs.append(doc)

    if len(valid_docs) == 0:
        return np.array([])

    num_docs = len(valid_docs)
    doc_embeddings_list = valid_docs

    # Determine projection dimension (matching C++ logic)
    use_identity_proj = config.projection_type == ProjectionType.DEFAULT_IDENTITY
    if use_identity_proj:
        projection_dim = config.dimension
    else:
        if not config.projection_dimension or config.projection_dimension <= 0:
            raise ValueError(
                "A positive projection_dimension must be specified for non-identity projections"
            )
        projection_dim = config.projection_dimension

    # Configuration summary
    num_partitions = 2**config.num_simhash_projections

    # Document tracking
    doc_lengths = np.array([len(doc) for doc in doc_embeddings_list], dtype=np.int32)
    total_vectors = np.sum(doc_lengths)
    doc_boundaries = np.insert(np.cumsum(doc_lengths), 0, 0)
    doc_indices = np.repeat(np.arange(num_docs), doc_lengths)

    # Concatenate all embeddings
    all_points = np.vstack(doc_embeddings_list).astype(np.float32)

    # Pre-allocate output
    final_fde_dim = config.num_repetitions * num_partitions * projection_dim
    out_fdes = np.zeros((num_docs, final_fde_dim), dtype=np.float32)

    # Process each repetition
    for rep_num in range(config.num_repetitions):
        # rep_start_time = time.perf_counter()
        current_seed = config.seed + rep_num

        # Step 1: SimHash projection
        simhash_matrix = _simhash_matrix_from_seed(
            config.dimension, config.num_simhash_projections, current_seed
        )
        all_sketches = all_points @ simhash_matrix

        # Step 2: Apply dimensionality reduction if configured
        if use_identity_proj:
            projected_points = all_points
        elif config.projection_type == ProjectionType.AMS_SKETCH:
            ams_matrix = _ams_projection_matrix_from_seed(
                config.dimension, projection_dim, current_seed
            )
            projected_points = all_points @ ams_matrix
        else:
            raise ValueError(f"Unsupported projection type: {config.projection_type}")

        # Step 3: Vectorized partition index calculation
        bits = (all_sketches > 0).astype(np.uint32)
        partition_indices = np.zeros(total_vectors, dtype=np.uint32)

        # Vectorized Gray Code computation
        for bit_idx in range(config.num_simhash_projections):
            partition_indices = (partition_indices << 1) + (
                bits[:, bit_idx] ^ (partition_indices & 1)
            )

        # Step 4: Vectorized aggregation
        # Initialize storage for this repetition
        rep_fde_sum = np.zeros(
            (num_docs * num_partitions * projection_dim,), dtype=np.float32
        )
        partition_counts = np.zeros((num_docs, num_partitions), dtype=np.int32)

        # Count vectors per partition per document
        np.add.at(partition_counts, (doc_indices, partition_indices), 1)

        # Aggregate vectors using flattened indexing for efficiency
        doc_part_indices = doc_indices * num_partitions + partition_indices
        base_indices = doc_part_indices * projection_dim

        for d in range(projection_dim):
            flat_indices = base_indices + d
            np.add.at(rep_fde_sum, flat_indices, projected_points[:, d])

        # Reshape for easier manipulation
        rep_fde_sum = rep_fde_sum.reshape(num_docs, num_partitions, projection_dim)

        # Step 5: Convert sums to averages (for document FDE)
        # Vectorized division where counts > 0
        non_zero_mask = partition_counts > 0
        counts_3d = partition_counts[:, :, np.newaxis]  # Broadcasting for division

        # Safe division (avoid divide by zero)
        np.divide(rep_fde_sum, counts_3d, out=rep_fde_sum, where=counts_3d > 0)

        # Fill empty partitions if configured
        empty_filled = 0
        if config.fill_empty_partitions:
            empty_mask = ~non_zero_mask
            empty_docs, empty_parts = np.where(empty_mask)

            for doc_idx, part_idx in zip(empty_docs, empty_parts):
                if doc_lengths[doc_idx] == 0:
                    continue

                # Get sketches for this document
                doc_start = doc_boundaries[doc_idx]
                doc_end = doc_boundaries[doc_idx + 1]
                doc_sketches = all_sketches[doc_start:doc_end]

                # Vectorized distance calculation
                binary_rep = _gray_code_to_binary(part_idx)
                target_bits = (
                    binary_rep >> np.arange(config.num_simhash_projections - 1, -1, -1)
                ) & 1
                distances = np.sum(
                    (doc_sketches > 0).astype(int) != target_bits, axis=1
                )

                nearest_local_idx = np.argmin(distances)
                nearest_global_idx = doc_start + nearest_local_idx

                rep_fde_sum[doc_idx, part_idx, :] = projected_points[nearest_global_idx]
                empty_filled += 1

        # Step 6: Copy results to output array
        rep_output_start = rep_num * num_partitions * projection_dim
        out_fdes[
            :, rep_output_start : rep_output_start + num_partitions * projection_dim
        ] = rep_fde_sum.reshape(num_docs, -1)

    # Step 7: Apply final projection if configured
    if config.final_projection_dimension and config.final_projection_dimension > 0:
        # Process in chunks to avoid memory issues
        chunk_size = min(100, num_docs)
        final_fdes = []

        for i in range(0, num_docs, chunk_size):
            chunk_end = min(i + chunk_size, num_docs)
            chunk_fdes = np.array(
                [
                    _apply_count_sketch_to_vector(
                        out_fdes[j], config.final_projection_dimension, config.seed
                    )
                    for j in range(i, chunk_end)
                ]
            )
            final_fdes.append(chunk_fdes)

        out_fdes = np.vstack(final_fdes)

    return out_fdes


def generate_query_fde_batch(
    queries: Sequence[Embeddings], config: FixedDimensionalEncodingConfig
) -> List[Embedding]:
    """
    Generates FDEs for a batch of queries using highly optimized NumPy vectorization.
    Fully compliant with C++ implementation including all projection types.
    """
    if config.fill_empty_partitions:
        raise ValueError("Queries must not use fill_empty_partitions=True.")
    return [generate_query_fde(query, config) for query in queries]


def create_document_fdes(
    documents: Sequence[Embeddings],
    *,
    dims: int,
    k_sim: int = 4,
    proj_dim: Optional[int] = None,
    num_repetitions: int = 2,
    seed: int = 42,
    fill_empty_partitions: bool = False,
    final_dim: Optional[int] = None,
) -> Embeddings:
    """Create Fixed Dimensional Encodings for document multivectors."""
    config = _build_fde_config(
        dims=dims,
        k_sim=k_sim,
        proj_dim=proj_dim,
        num_repetitions=num_repetitions,
        seed=seed,
        fill_empty_partitions=fill_empty_partitions,
        final_dim=final_dim,
        encoding_type=EncodingType.AVERAGE,
    )
    # Just delegate to the real batch generator
    return generate_document_fde_batch(documents, config)


def create_query_fdes(
    queries: Sequence[Embeddings],
    *,
    dims: int,
    k_sim: int = 4,
    proj_dim: Optional[int] = None,
    num_repetitions: int = 2,
    seed: int = 42,
    final_dim: Optional[int] = None,
) -> Embeddings:
    """Create Fixed Dimensional Encodings for query multivectors."""
    config = _build_fde_config(
        dims=dims,
        k_sim=k_sim,
        proj_dim=proj_dim,
        num_repetitions=num_repetitions,
        seed=seed,
        fill_empty_partitions=False,
        final_dim=final_dim,
        encoding_type=EncodingType.DEFAULT_SUM,
    )
    return generate_query_fde_batch(queries, config)


def create_fdes(
    multivectors: Sequence[Embeddings],
    *,
    dims: int,
    k_sim: int = 4,
    proj_dim: Optional[int] = None,
    num_repetitions: int = 2,
    seed: int = 42,
    fill_empty_partitions: bool = False,
    final_dim: Optional[int] = None,
    is_query: bool = False,
) -> Embeddings:
    """Convenience wrapper that generates query or document FDEs based on `is_query`."""
    if is_query:
        return create_query_fdes(
            multivectors,
            dims=dims,
            k_sim=k_sim,
            proj_dim=proj_dim,
            num_repetitions=num_repetitions,
            seed=seed,
            final_dim=final_dim,
        )

    return create_document_fdes(
        multivectors,
        dims=dims,
        k_sim=k_sim,
        proj_dim=proj_dim,
        num_repetitions=num_repetitions,
        seed=seed,
        fill_empty_partitions=fill_empty_partitions,
        final_dim=final_dim,
    )

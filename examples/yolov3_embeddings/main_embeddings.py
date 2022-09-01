from __future__ import annotations
import uuid
import json
import random

import tqdm
import numpy as np

import torch
from torch.utils.data import DataLoader
from torch.autograd import Variable
from pytorchyolo.utils.utils import bbox_iou

from pytorchyolo.models import load_model
from pytorchyolo.utils.utils import load_classes, non_max_suppression, xywh2xyxy, rescale_boxes
from pytorchyolo.utils.datasets import ListDataset
from pytorchyolo.utils.transforms import DEFAULT_TRANSFORMS
from pytorchyolo.utils.parse_config import parse_data_config

from chroma.sdk import chroma_manager
from chroma.sdk.utils import nn

str_options = ["New York", "San Francisco", "Atlanta", "Miami", "Dallas", "Chicago", "DC"]


def reshape_detections(inputs, num_anchors, no):
    bs, _, ny, nx = inputs.shape  # x(bs,255,_,_) to x(bs,3,_,_,85)
    inputs = inputs.view(bs, num_anchors, no, ny, nx).permute(0, 1, 3, 4, 2).contiguous()
    inputs = inputs.view(bs, -1, no)
    return inputs


def reshape_context(inputs):
    avg_pool_module = torch.nn.AvgPool2d(kernel_size=inputs.shape[2])
    return avg_pool_module(inputs)[:, :, -1, -1]


def to_annotations_dict(boxes, category_ids, category_names, uids, metadatas):
    annotations = []

    for box, category_id, category_name, uid, metadata in zip(
        boxes, category_ids, category_names, uids, metadatas
    ):
        annotation = {
            "id": uid,
            "bbox": box.tolist(),
            "category_id": str(category_id),
            "category_name": category_name,
            "metadata": metadata,
        }
        annotations.append(annotation)

    return {"annotations": annotations}


def infer(model, device, data_loader, class_names, chroma_storage: chroma_manager.ChromaSDK):

    context_layer_index = 74  # Output of last layer in backbone network
    detection_layer_indices = [81, 93, -2]  # Outputs of last conv. layers before YOLO layers
    num_anchors = 3
    no = 85

    conf_thres = 0.01
    nms_thres = 0.4

    Tensor = torch.FloatTensor
    if device.type == "cuda":
        Tensor = torch.cuda.FloatTensor
    elif device.type == "mps":
        Tensor = torch.backends.mps.torch.FloatTensor

    class_names = np.array(class_names)

    for img_paths, imgs, targets, img_sizes in tqdm.tqdm(data_loader, desc="Processing"):
        img_sizes = np.array(img_sizes)
        chroma_storage.set_resource_uris(uris=list(img_paths))

        imgs = Variable(imgs.type(Tensor), requires_grad=False).to(device)

        with torch.no_grad():
            outputs, layer_outputs = model(imgs)
            outputs, output_indices = non_max_suppression(
                outputs, conf_thres=conf_thres, iou_thres=nms_thres
            )

        detection_inferences = []
        labels = []
        metadata_list = []
        for i, output in enumerate(outputs):

            # Process targets
            target = targets[targets[:, 0] == i]
            target_boxes = rescale_boxes(
                xywh2xyxy(target[:, 2:]) * imgs.shape[-1], imgs.shape[-1], img_sizes[i, :2]
            )
            # convert back to xywh... I am sure there is a better way of doing this
            for target_box in target_boxes:
                target_box[2] = target_box[2] - target_box[0]
                target_box[3] = target_box[3] - target_box[1]
            target_cat_ids = target[:, 1].numpy().astype(int)
            target_cat_names = class_names[target_cat_ids]
            target_uids = [str(uuid.uuid4()) for i in range(len(target_cat_ids))]
            metadatas = [
                {"quality": random.randint(0, 100), "location": str_options[random.randint(0, 6)]}
                for i in range(len(target_cat_ids))
            ]
            target_annotations = to_annotations_dict(
                target_boxes, target_cat_ids, target_cat_names, target_uids, metadatas
            )
            labels.append(target_annotations)

            # Process detections
            det_boxes = rescale_boxes(output, imgs.shape[-1], img_sizes[i, :2])[:, :4]
            det_boxes[:, 2:4] = det_boxes[:, 2:4] - det_boxes[:, 0:2]  # xyxy2xywh
            det_category_ids = output[:, -1].numpy().astype(int)
            det_category_names = class_names[det_category_ids]
            det_uids = [str(uuid.uuid4()) for i in range(len(det_category_ids))]

            # Associate detections with labels
            # We do this without rescaling to the original image, in the shape of the input
            assoc_iou_threshold = 0.5
            target[:, 2:] = xywh2xyxy(target[:, 2:])
            target[:, 2:] *= imgs.shape[-1]

            annotations = target[:, 1:]
            target_labels = annotations[:, 0] if len(annotations) else []
            pred_boxes = output[:, :4]
            pred_scores = output[:, 4]
            pred_labels = output[:, -1]

            assocs = {}
            ious = {}
            scores = {}
            if len(annotations):
                for pred_i, (pred_box, pred_label) in enumerate(zip(pred_boxes, pred_labels)):
                    scores[pred_i] = pred_scores[pred_i].item()
                    # Skip if we've found all the annotations
                    if len(assocs) == len(annotations):
                        continue
                    # Reject class mismatch
                    if pred_label not in target_labels:
                        continue

                    # Filter target_boxes by pred_label so that we only match against boxes of our own label
                    filtered_target_position, filtered_targets = zip(
                        *filter(
                            lambda x: target_labels[x[0]] == pred_label, enumerate(target_boxes)
                        )
                    )

                    # Find the best matching target for our predicted box
                    iou, target_box_filtered_index = bbox_iou(
                        pred_box.unsqueeze(0), torch.stack(filtered_targets)
                    ).max(0)

                    # Remap the index in the list of filtered targets for that label to the index in the list with all targets.
                    target_label_index = filtered_target_position[target_box_filtered_index]
                    if iou >= assoc_iou_threshold and target_label_index not in assocs.values():
                        assocs[pred_i] = target_label_index
                        ious[pred_i] = iou.item()

            metadatas = [
                {
                    "quality": random.randint(0, 100),
                    "location": str_options[random.randint(0, 6)],
                    "label_id": target_uids[assocs[i]] if i in assocs.keys() else "",
                    "iou": ious[i] if i in ious.keys() else "",
                    "score": scores[i] if i in scores.keys() else "",
                }
                for i in range(len(det_category_ids))
            ]
            det_annotations = to_annotations_dict(
                det_boxes, det_category_ids, det_category_names, det_uids, metadatas
            )
            detection_inferences.append(det_annotations)

            metadata_list.append(
                {"quality": random.randint(0, 100), "location": str_options[random.randint(0, 6)]}
            )

        # raise Exception(str(labels))
        chroma_storage.set_inferences(detection_inferences)
        chroma_storage.set_labels(labels)
        chroma_storage.set_metadata(metadata_list)

        ctx_embedding = reshape_context(layer_outputs[context_layer_index]).detach().cpu().numpy()

        detection_embeddings = (
            torch.cat(
                tensors=[
                    reshape_detections(inputs=layer_outputs[i], num_anchors=num_anchors, no=no)
                    for i in detection_layer_indices
                ],
                dim=1,
            )
            .detach()
            .cpu()
            .numpy()
        )

        filtered_embs = []
        for i, output_idx in enumerate(output_indices):
            if (type(output_idx) != torch.Tensor) or (output_idx.nelement() != 0):
                embs = detection_embeddings[i][output_idx][:]
                # Account for only one detection
                if len(embs.shape) == 1:
                    embs = np.expand_dims(embs, 0)
                filtered_embs.append(embs)
            else:
                filtered_embs.append([])

        annotated_embs = []
        for det_annotations, embs in zip(detection_inferences, filtered_embs):
            annotated_emb = [
                {"target": det_annotation["id"], "data": emb.tolist()}
                for det_annotation, emb in zip(det_annotations["annotations"], embs)
            ]
            annotated_embs.append(annotated_emb)

        annotated_ctx_embs = []
        for ctx in ctx_embedding:
            annotated_ctx_emb = [{"target": None, "data": ctx.tolist()}]
            annotated_ctx_embs.append(annotated_ctx_emb)

        chroma_storage.set_ctx_embeddings(annotated_ctx_embs)
        chroma_storage.set_embeddings(annotated_embs)
        chroma_storage.store_batch_embeddings()

    return


def main():
    # Setup params
    model_path = "config/yolov3.cfg"
    weights_path = "weights/yolov3.weights"
    data = "config/coco1000.data"
    batch_size = 8
    img_size = 416
    n_cpu = 8

    use_cuda = False
    use_mps = False

    data_config = parse_data_config(data)
    valid_path = data_config["valid"]
    class_names = load_classes(data_config["names"])

    class_object = []
    i = 0
    for class_name in class_names:
        class_object.append({"supercategory": "", "id": i, "name": class_name})
        i = i + 1

    # Set the device
    device_name = "cpu"
    if use_mps & hasattr(torch.backends, "mps") & torch.backends.mps.is_available():
        device_name = "mps"
    if use_cuda & torch.cuda.is_available():
        device_name = "cuda"
    device = torch.device(device_name)

    dataset = ListDataset(
        valid_path, img_size=img_size, multiscale=False, transform=DEFAULT_TRANSFORMS
    )
    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=False,
        num_workers=n_cpu,
        pin_memory=True,
        collate_fn=dataset.collate_fn,
    )

    model = load_model(model_path=model_path, device=device, weights_path=weights_path)
    model.eval()

    with chroma_manager.ChromaSDK(
        project_name="YOLO-1k", dataset_name="TestYolo1k", categories=json.dumps(class_object)
    ) as chroma_storage:
        infer(model, device, dataloader, class_names, chroma_storage=chroma_storage)


if __name__ == "__main__":
    main()

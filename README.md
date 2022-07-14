<img src="./assets/chroma.png" width="300px">

**_Chroma is currently in alpha_**

# What is Chroma ?

Chroma is an open-source python package and web-based UI which automatically detects anomalies in your ML datasets. It lets you see your data the way your model sees it, so you know what to add, remove, or change, in order to improve model performance and accelerate validation and deployment.

With the alpha version of Chroma, you can:

- Find bad labels among your training data
- Find unusual examples in any of your data
- Look for clusters of examples which confuse your model

Chroma is under active development, with many more features coming soon!

# Try it out with MNIST

Let's explore the MNIST dataset with Chroma! We've trained a simple CNN classifier model on the MNIST digits training set. Now we'd like to see how we can improve our model by finding dataset anomalies.

## Quick Start

Chroma runs locally on your system - there's no need to send your datasets anywhere else.

0. Unzip the Chroma archive. Since you're reading this, you've already done that.

1. Chroma needs Python 3.9+ and [Redis installed](https://redis.io/docs/getting-started/installation/) to work. Please make sure these requirements are setup for your system.

2. Set up and activate a python virtual environment:

   With conda:

   ```
    conda create --name chroma_env python=3.9
    conda activate chroma_env
   ```

   With venv:

   ```
   python3 -m venv chroma_env
   source chroma_env/bin/activate
   ```

   You can also use an existing environment if you prefer, but Chroma will install its dependencies.

3. Install the Chroma package. Within the directory where you unzipped Chroma:

   ```
   pip install .
   ```

4. Preload the data:

   To get you started faster, we've created a pre-filled database containing Chroma information about the MNIST test and training datasets.

   ```
   make preload-data
   ```

5. Run the Chroma backend:

   ```
   make run
   ```

6. Open the Chroma GUI in a web browser:

   ```
   open http://localhost:8000

   ```

## Explore!

Now it's time to explore the MNIST datasets with Chroma. When you open the MNIST project, you will see the main chroma UI. Each point represents one piece of data, as seen by your model - these are 2D projections of the data in the model's high-dimensional latent space.

The points start off colored by the inference the model produced, in this case, which digit from 0-9 the model predicted for a given input. Try coloring the points by Dataset instead - click on the dropdown in the top left of the plot. To see which color represents which dataset, you may need to scroll down in the left panel.

> Do the training and test sets overlap? Does the training set have good coverage of the test set?

You can also color by Label (if the data point has one, e.g. because it came from a training set, or was triaged), by Quality (a measure of how close to other points of the same class each point is), or by whether the Label class matches the Inferred class.

Let's get more information about how our model is doing, and look at some of the examples that confuse it. On the left panel, click 'Agree' under 'Label/Inference Match'. This will hide the set of points where the label agrees with the inference - only the points where the label and inference disagree are left. You can hide any set of points by clicking on the name of the set on the left bar. The data in the Inspection panel on the right now shows only points where label and inference disagree, i.e. the model is confused.

> Scroll through the data on the right - are there any obviously mislabeled examples in the dataset? Is the data strange in some other way?

You can add tags to any individual datapoint by clicking on it, and entering a tag in the Tags field. Multiple tags can be added by separating them with a comma.

Let's zoom in on a single class, and see where the model is confused about it. In the left panel, to the right of each set name, there is a circular selection icon. Clicking this icon will select all visible points in that set.

> Select the set of Inference for '1', with the 'Agree' points still hidden. Take a look at the data on the right - do any examples seem unusual? Are there more bad labels or bad input data? Is there a common type of mistake the model seems to make?

You can add tags to all selected datapoints at once, by entering them in the top right. You can also remove them from an entire selection by name. Tags form a set which can be hidden or selected in the same way.

Selecting and hiding various combinations of points and sets lets you explore the data in a large variety of ways.

Let's expore the datapoints in a more fine-grained way. You can select an individual point by clicking on it, to see information about it in the Inspection panel on the right. You can also select multiple points using the lasso tool - click on it in the top left of the UI. Try lassoing an interesting looking cluster of points.

> Can you identify clusters representing 1's with a foot, or 7's with a bar? Try tagging them.

Finally, Chroma lets you get automated insights about your data. Let's try looking for anything unusual in the test set - hide the training dataset, and show everything else. In the top right of the Inspection panel, select Quality - up. This sorts all the data from the training set by the Quality score, a measure of how close to other points of the same class each point is in the high dimensional latent space of the model.

> Are there more unusual examples in the test set, even when the inference agrees with the label? What about in the training set? Should some data be removed? Do we need more examples of other data?

Have fun, and try to come up with some more insights of your own!

---

# Chroma for Your Models

The data in this demo is pre-loaded, but with a little setup, Chroma works with your models too!

## How it works

Chroma runs locally in your training and production inference environments. On every forward pass of your model, Chroma records each data point in the model's latent space, and associates it with the corresponding input data and output inference.

Chroma currently supports PyTorch, but integration with other frameworks is under active development. Chroma connects to any model trained from labeled data with only a few lines of code. Let us show you how!

The power of Chroma for ML model development is two-fold:

- It performs automated analysis in the high dimensional latent space, to generate automated insights like the Quality score in the MNIST demo.

- It displays the high dimensional data in a human readable and meaningful way, allowing humans to identify and interpret patterns that the model overlooks.

And that's just the start.

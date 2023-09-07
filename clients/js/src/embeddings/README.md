# Embeddings Function Definition

Embedding functions are used to transform text data into numerical representations, which are easier for machine learning models to process. Here's how you can define an embedding function.

## Step 1: Create a Folder for the Embeddings Function

First, you need to create a new folder inside `clients/js/src/embeddings`. This folder should be named after the embeddings organization. For example, if you're using embeddings from OpenAI, you might name the folder `openai`. 

For example, if you are creating your own embeddings function named `MyEmbeddings`, you should create a folder at `clients/js/src/embeddings/my-embeddings`.

## Step 2: Import the BaseEmbeddingFunction

Next, inside this new folder, create a TypeScript file (for instance, `MyEmbeddingFunction.ts`) and import the `BaseEmbeddingFunction` class from the appropriate module. This class defines the structure that your embedding function should follow.

```typescript
import { BaseEmbeddingFunction } from "../BaseEmbeddingFunction";
```

## Step 3: Define Your Class

Now, you'll need to define a class that implements the `BaseEmbeddingFunction` class. This class will contain the logic for your embedding function.

Here's an example of what this could look like:

```typescript

export type MyEmbeddingFunctionOptions = {
  myApiKey: string,
}

export class MyEmbeddingFunction extends BaseEmbeddingFunction {

  constructor(options: MyEmbeddingFunctionOptions, embeddingLibrary){
    //💡you can later use this embedding library using this.modules.embeddingLibrary. Take a look at BaseEmbeddingFunction to learn how it works.
    super(options, {embeddingLibrary})
  }

  // Your code here
}
```

## Step 4: Implement the `generate` Method

The `BaseEmbeddingFunction` class requires a `generate` method that takes an array of strings and returns a promise that resolves to a 2D array of numbers. Since we do not ship 3rd party packages which are used to generate the embeddings, one must pass it via constructor. Don't use any optional dependency in your generate function, instead use the one passed via constructor.

### Do ✅
```typescript
public async generate(texts: string[]): Promise<number[][]> {
  // Your code here, for example:
  return this.modules.embeddingLibrary.generateEmbeddings(texts);
}

```

### Don't ❌
```typescript
public async generate(texts: string[]): Promise<number[][]> {
  // Your code here, for example:
  return embeddingLibrary.generateEmbeddings(texts);
}
```

In this method, you should implement the logic to transform the input texts into numerical representations. This will typically involve using some sort of machine learning model.

## Step 5: Export Your Class

Finally, export your class so that it can be imported and used elsewhere in your project.

```typescript
export { MyEmbeddingFunction };
```

## Step 6: Usage

After defining the embedding function, the build script in the `package.json` will generate files so that you can import it using the following syntax:

```typescript
import { MyEmbeddingFunction } from 'chromadb/my-embeddings';
```

## Step 7 - Optional
### Implement the .init function for more flexibility

The BaseEmbeddingFunction define an init function which can be an alternative way of loading the 3rd party module required to generate the embeddings. This might be useful, when you need to initialize the class of a module with special parameters to make it work like in the WebAI embeddings function.
When you implement this, you might want to make the embedding library optional.

```typescript
export class MyEmbeddingFunction extends BaseEmbeddingFunction<MyEmbeddingFunctionOptions, { embeddingLibrary: any }> {

  constructor(options: MyEmbeddingFunctionOptions, embeddingLibrary = default /*💡make it optional */){
    super(options, {embeddingLibrary})
  }

  public async init(): Promise<void> {
    this.modules = {
      // 💡import the module here, you may load multiple ones
      embeddingLibrary: await import('embedding-library'),
      embeddingLibraryExtended: await import('embedding-library/extended'),
    }
  }

  public async generate(texts: string[]): Promise<number[][]> {
    // Your code here, for example:
    if(this.options.useExtendedGenerator){
      return this.modules.embeddingLibraryExtended.generateEmbeddings(texts);
    } else {
      return this.modules.embeddingLibrary.generateEmbeddings(texts);
    }
  }
}


```
### Using multiple generator functions

You might want to use multiple generators for e.g. multimodal generators. Here is an example of how you could implement that.

```typescript
export class MyEmbeddingFunction extends BaseEmbeddingFunction<MyEmbeddingFunctionOptions, { textEmbeddingsLibrary, imageEmbeddingsLibray }> {

  constructor(options: MyEmbeddingFunctionOptions, { textEmbeddingsLibrary, imageEmbeddingsLibray } /* 💡require multiple libraries here */{
    super(options, { textEmbeddingsLibrary, imageEmbeddingsLibray })
  }

  public async generate(texts: string[]): Promise<number[][]> {
    // Your code here, for example:
    if(this.options.modality === 'image'){
      return this.modules.imageEmbeddingsLibray.generateEmbeddings(texts);
    } else {
      return this.modules.textEmbeddingsLibrary.generateEmbeddings(texts);
    }
  }
}
```

### Define Node.js and Browser Environments

You might need different implementations for node and browser environments. Take a look at other embedding functions. OpenAI embedding function requires a special flag for that and the WebAI embedding function uses different packages for node and browser environments.

## Conclusion

That's it! You've now defined an embedding function. This function can now be used to transform text data into a format that's suitable for machine learning models.

Remember, the specific logic in your `generate` method will depend on the machine learning model you're using and the specific requirements of your project.
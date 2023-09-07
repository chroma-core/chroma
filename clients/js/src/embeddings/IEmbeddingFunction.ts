export class BaseEmbeddingFunction<Options, Modules> {
    // Define your configuration here
    protected options?: Options;
    protected modules: Modules | undefined;

    constructor(options: Options | undefined, modules: Modules){
        this.options = options;
        this.modules = modules;
    }

    public generate(texts: string[]): Promise<number[][]>{
        return Promise.reject('This is not implementet, please report this to the chromadb team.')
    }

    public init(target?: 'node' | 'browser'): Promise<void>{
        return Promise.reject('This is not implementet, please pass the module via constructor. You might may pass an initialized module if an initialization of a module class is required.')
    };
}

General:

- remove all if (err || ) statements in favor of explicit ones

reader.js:

-

    let readFn = () => parquet_util.fread(fileDescriptor);
    let closeFn = () => parquet_util.fclose(fileDescriptor);



- codec
   get rid of weird decodeValues_Something in favor of object or map
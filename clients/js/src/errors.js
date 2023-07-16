class ChromaError extends Error {
    constructor(message) {
        super(message);
        this.name = "ChromaError";
    }
}

class InvalidDimensionException extends ChromaError {
    constructor(message = "Invalid dimension") {
        super(message);
        this.name = "InvalidDimension";
    }
}

class InvalidCollectionException extends ChromaError {
    constructor(message = "Invalid collection") {
        super(message);
        this.name = "InvalidCollection";
    }
}

class IDAlreadyExistsError extends ChromaError {
    constructor(message = "ID already exists") {
        super(message);
        this.name = "IDAlreadyExists";
    }
}

class DuplicateIDError extends ChromaError {
    constructor(message = "Duplicate ID") {
        super(message);
        this.name = "DuplicateID";
    }
}

class InvalidUUIDError extends ChromaError {
    constructor(message = "Invalid UUID") {
        super(message);
        this.name = "InvalidUUID";
    }
}

module.exports = {
    ChromaError,
    InvalidDimensionException,
    InvalidCollectionException,
    IDAlreadyExistsError,
    DuplicateIDError,
    InvalidUUIDError
};
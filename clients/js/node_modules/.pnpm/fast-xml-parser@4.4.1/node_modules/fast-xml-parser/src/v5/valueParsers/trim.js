class trimmer{
    parse(val){
        if(typeof val === "string") return val.trim();
        else return val;
    }
}

module.exports = trimmer;
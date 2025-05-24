/**
 * 
 * @param {array} val 
 * @param {string} by 
 * @returns 
 */
function join(val, by=" "){
    if(isArray(val)){
        val.join(by)
    }
    return val;
}

module.exports = join;
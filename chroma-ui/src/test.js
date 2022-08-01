
// console.time('write object')
// var object = {}
// for (let index = 0; index < 1_000_000; index++) {
//     object[index] = {
//         id: Math.random(),
//         data: "test"
//     }
// }

// console.timeEnd('write object')

// // console.log('object', object)

// console.time('write array')
// var arr = []
// for (let index = 0; index < 1_000_000; index++) {
//     arr.push({
//         id: Math.random(),
//         data: "test"
//     })
// }

// // console.log('arr', arr)
// console.timeEnd('write array')

// var selectedIds = []
// for (let index = 0; index < 10_000; index++) {
//     selectedIds.push((Math.ceil(Math.random() * 100000)))
// }
// // console.log('selectedIds', selectedIds)

// console.time('read object')

// const selectedPoints = selectedIds.map((id) => {
//     return object[id]
// })
// console.log('selectedPoints', selectedPoints)

// console.timeEnd('read object')

// console.time('read array')

// const selectedPoints2 = selectedIds.map((id) => {
//     return arr[id]
// })
// console.log('selectedPoints2', selectedPoints2)

// console.timeEnd('read array')

var object = {}
for (let index = 0; index < 1_000; index++) {
    object[index] = {
        id: Math.random(),
        data: "test"
    }
}

const t0 = performance.now();
Object.keys(object).map(function (keyName, keyIndex) {
    // console.log('keyName', keyName, 'keyIndex', keyIndex, 'object[keyName]', object[keyName])
})
const t1 = performance.now();
console.log(`Call to object.keys took ${t1 - t0} milliseconds.`);

const t3 = performance.now();
Object.values(object).map(function (val, keyIndex) {
    // console.log('val', val)
})
const t4 = performance.now();
console.log(`Call to object.values took ${t4 - t3} milliseconds.`);

console.log((Object.keys(object)[1]))
console.log(object[1])
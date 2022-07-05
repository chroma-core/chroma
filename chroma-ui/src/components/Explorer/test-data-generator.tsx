const classes = {
  forest: 0,
  sky: 1,
  buildings: 2,
}
const types = {
  test: 0,
  production: 1,
  triage: 2,
}
const metadata_modelVersion = {
  v1: 0,
  v2: 1,
  v3: 2,
}

var randomProperty = function (obj: { [key: string]: any }) {
  var keys = Object.keys(obj)
  return keys[(keys.length * Math.random()) << 0]
}
var randomValue = function (obj: { [key: string]: any }) {
  var keys = Object.keys(obj)
  return obj[keys[(keys.length * Math.random()) << 0]]
}

function smallNumPoints(num: number) {
  return new Array(num)
    .fill(0)
    .map(() => (
      {
        x: -1 + Math.random() * 2,
        y: -1 + Math.random() * 2,
        metadata: JSON.stringify({
          'class': randomProperty(classes),
          'type': randomProperty(types),
          'ml_model_version': randomProperty(metadata_modelVersion)
        })
      }
    )
    );
}

export { smallNumPoints }

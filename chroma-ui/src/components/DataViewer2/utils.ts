export const preprocess = (datapoints: any) => {
  datapoints.map((dp:any) => {

    // our HABTM models are fetched oddly, this is a hack to fix that
    // @ts-ignore
    let newTags = []
    if (dp.tags.length > 0) {
      dp.tags.map((t:any) => {
        newTags.push(t.tag)
      })
      // @ts-ignore
      dp.tags = newTags
    }

    // open up our inference and label json
    const labelData = JSON.parse(dp.label.data)
    dp.annotations = labelData.annotations
    dp.categories = labelData.categories

    // parse metadata
    dp.metadata = JSON.parse(dp.metadata_)

    // inject projection for now..........
    dp.projection = {id: dp.id, x: Math.random()*10, y: Math.random()*10}
  })

  return datapoints
}

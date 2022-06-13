cd ..

if ! (git clone git@github.com:chroma-core/dom-2d-camera.git) then
    echo "Unable to fetch dom-2d-camera, perhaps you already have it"
    # Put Failure actions here...
else
    echo "Fetched dom-2d-camera"
    cd dom-2d-camera
    yarn install
    yarn build
    cd ..
    # Put Success actions here...
fi

if ! (git clone git@github.com:chroma-core/regl-scatterplot.git) then
    echo "Unable to fetch regl-scatterplot, perhaps you already have it"
    # Put Failure actions here...
else
    echo "Fetched regl-scatterplot"
     cd regl-scatterplot
    yarn install
    yarn build
    cd ..
    # Put Success actions here...
fi
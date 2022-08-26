import { v4 as uuidv4 } from 'uuid';
import {
  Input,
  InputGroup,
  InputLeftElement,
  useColorModeValue,
  useTheme
} from '@chakra-ui/react'
import React, { useState } from 'react'
import { BsTagFill, BsTag } from 'react-icons/bs'
import { useAppendTagByNameToDatapointsMutation, useRemoveTagFromDatapointsMutation } from '../../graphql/graphql'
import { contextObjectSwitcherAtom, context__datapointsAtom, DataType, selectedDatapointsAtom, context__tagsAtom, globalSelectedDatapointsAtom, globalTagsAtom, globalDatapointAtom } from './atoms'
import { datapointIndexToPointIndex } from './DataViewerUtils'
import { useAtom } from 'jotai'
import { removeItem } from './Tags'

const TagForm: React.FC = () => {
  const [datapoints, updatedatapoints] = useAtom(globalDatapointAtom)
  const [tags, updatetags] = useAtom(globalTagsAtom)
  const [selectedDatapoints] = useAtom(globalSelectedDatapointsAtom)
  const [contextObjectSwitcher] = useAtom(contextObjectSwitcherAtom)

  const theme = useTheme()
  const textColor = useColorModeValue(theme.colors.ch_gray.dark, theme.colors.ch_gray.light)
  const noneSelected = selectedDatapoints.length === 0

  // state for the inputs
  const [newTag, setNewTag] = useState("")
  const [newUnTag, setNewUnTag] = useState("")

  // mutations
  const [addTagResult, addTag] = useAppendTagByNameToDatapointsMutation()
  const [unTagResult, unTag] = useRemoveTagFromDatapointsMutation()

  // callback for a new tag
  const onSubmitTagAll = (e: any) => {
    e.preventDefault()

    let splitNewTags = newTag.split(",")
    var selectedDatapointsCopy = selectedDatapoints.slice()

    // get selected datapoint ids from selected projection ids
    var selectedPointsIds = selectedDatapoints.slice().map(selectedPointId => {
      return datapointIndexToPointIndex(selectedPointId)
    })

    var targetIds: any = null
    var objectDpIds: any = null
    if (contextObjectSwitcher == DataType.Object) {
      targetIds = selectedDatapointsCopy.map(sd => datapoints[sd].annotations[0].id)
      // @ts-ignore
      objectDpIds = selectedDatapointsCopy.map(sd => datapoints[sd].source_datapoint_id)
    }

    // add the new tags to each datapoint
    splitNewTags.map(tag => {
      let target = (contextObjectSwitcher == DataType.Object) ? targetIds : null
      let datapointIds = (contextObjectSwitcher == DataType.Object) ? objectDpIds : selectedDatapointsCopy
      const variables = { tagName: tag, datapointIds: datapointIds, target: target }
      addTag(variables)

      var tempUUid = uuidv4()
      splitNewTags.map(t => {
        var exists = Object.values(tags).findIndex(existingTag => existingTag.name == t.trim()) // -1 means it doesnt exist yet, otherwise we need the index
        if (exists < 0) {
          // add and get the index
          // @ts-ignore
          tags[tempUUid] = { id: tempUUid, name: t.trim(), datapoint_ids: selectedDatapoints }
          selectedDatapoints.map(d => {
            // @ts-ignore
            datapoints[d].tag_ids.push(tags[tempUUid].id)
          })
        } else {
          // add to the tag
          Object.values(tags)[exists].datapoint_ids.push(...selectedDatapoints)
          // @ts-ignore
          selectedDatapoints.map(d => {
            // @ts-ignore
            datapoints[d].tag_ids.push(Object.keys(tags)[exists].id)
          })
        }
      })
    })

    updatetags({ ...tags })
    updatedatapoints({ ...datapoints })
    setNewTag("")
  }

  // callback for a new untag
  const onSubmitUntagAll = (e: any) => {
    e.preventDefault()

    let splitNewUnTags = newUnTag.split(",")

    var selectedDatapointsCopy = selectedDatapoints.slice()
    var newTags = Object.assign({}, tags)
    var newDatapoints = Object.assign({}, datapoints)

    var targetIds: any = null
    var objectDpIds: any = null
    if (contextObjectSwitcher == DataType.Object) {
      targetIds = selectedDatapointsCopy.map(sd => datapoints[sd].annotations[0].id)
      // @ts-ignore
      objectDpIds = selectedDatapointsCopy.map(sd => datapoints[sd].source_datapoint_id)
    }

    var markForDeletion: number[] = []
    splitNewUnTags.map(tag => {

      let target = (contextObjectSwitcher == DataType.Object) ? targetIds : null
      let datapointIds = (contextObjectSwitcher == DataType.Object) ? objectDpIds : selectedDatapointsCopy
      const variables = { tagName: tag, datapointIds: datapointIds, target: target }
      unTag(variables)

      var tagIndex = Object.values(newTags).findIndex(existingTag => existingTag.name == tag) // -1 means it doesnt exist yet, otherwise we need the index

      selectedDatapointsCopy.forEach(sd => {

        let tagDatapoints = Object.values(newTags)[tagIndex].datapoint_ids
        let tagDatapointsNew = tagDatapoints.slice()
        let tagId = Object.values(newTags)[tagIndex].id
        let datapointId = newDatapoints[sd].id
        let datapointTags = newDatapoints[sd].tag_ids
        let index = 0

        index = datapointTags.indexOf(tagId);
        if (index > -1) {
          datapointTags.splice(index, 1);
        }

        index = tagDatapoints.indexOf(datapointId);
        if (index > -1) {
          tagDatapointsNew.splice(index, 1);
        }
        if (tagDatapointsNew.length === 0) {
          markForDeletion.push(tagId)
        }

        Object.values(newTags)[tagIndex].datapoint_ids = tagDatapointsNew

      })
    })

    markForDeletion.map(deleteTagId => {
      delete newTags[deleteTagId]
    })

    updatetags({ ...newTags })
    updatedatapoints({ ...newDatapoints })

    setNewUnTag("")
  }

  // input sanitiziation, direct passthrough right now
  const checkAndSetTag = (e: React.ChangeEvent<HTMLInputElement>, name: string) => {
    setNewTag(e.currentTarget.value)
  }

  // input sanitiziation, direct passthrough right now
  const checkAndSetUnTag = (e: React.ChangeEvent<HTMLInputElement>, name: string) => {
    setNewUnTag(e.currentTarget.value)
  }

  return (
    <>
      <form onSubmit={onSubmitTagAll} style={{ width: "100%" }}>
        <InputGroup ml={3} mt={2} mr={3} width="auto" pt={0}>
          <InputLeftElement
            pointerEvents='none'
            mt={-1}
            children={<BsTagFill color='gray.900' />}
          />
          <Input
            borderColor={"rgba(0,0,0,0)"}
            borderRadius={1}
            borderWidth={2}
            size="sm"
            onChange={(e: any) => checkAndSetTag(e, e.target.value)}
            isDisabled={noneSelected}
            value={newTag}
            _hover={{ borderColor: theme.colors.ch_gray.light }}
            _focus={{ borderColor: theme.colors.ch_blue }}
            _placeholder={{ opacity: 1, color: textColor }}
            placeholder='Tag selected' />
        </InputGroup>
      </form>

      <form onSubmit={onSubmitUntagAll} style={{ width: "100%" }}>
        <InputGroup ml={3} mr={3} width="auto" pt={0} mb={2}>
          <InputLeftElement
            pointerEvents='none'
            mt={-1}
            children={<BsTag color='gray.900' />}
          />
          <Input
            borderColor={"rgba(0,0,0,0)"}
            borderRadius={1}
            borderWidth={2}
            size="sm"
            value={newUnTag}
            onChange={(e: any) => checkAndSetUnTag(e, e.target.value)}
            isDisabled={noneSelected}
            _hover={{ borderColor: theme.colors.ch_gray.light }}
            _focus={{ borderColor: theme.colors.ch_blue }}
            _placeholder={{ opacity: 1, color: textColor }}
            placeholder='Untag selected' />
        </InputGroup>
      </form>
    </>
  )
}

export default TagForm


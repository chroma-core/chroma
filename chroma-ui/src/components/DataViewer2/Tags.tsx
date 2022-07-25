// @ts-nocheck
import { v4 as uuidv4 } from 'uuid';
import React, { useState, useEffect, useMemo, useCallback } from 'react'
import {
  useTheme,
  Textarea,
  Box,
  Text,
  Button,
} from '@chakra-ui/react'
import TagButton from './TagButton'
import { useAppendTagByNameToDatapointsMutation, useRemoveTagFromDatapointsMutation } from '../../graphql/graphql'
import { datapointsAtom, tagsAtom } from './atoms'
import { useAtom } from 'jotai'
import { useUpdateAtom } from 'jotai/utils'
import { focusAtom } from "jotai/optics";
// import { TagItem } from './DataPanel'

interface TagsProps {
  datapointId: number
}

export function removeItem<T>(arr: Array<T>, value: T): Array<T> {
  const index = arr.indexOf(value);
  if (index > -1) {
    arr.splice(index, 1);
  }
  return arr;
}

const Tags: React.FC<TagsProps> = ({ datapointId }) => {
  const [tags, setTags] = useAtom(tagsAtom)
  const theme = useTheme()
  const [isEditing, setIsEditing] = useState(false)
  const [originalTagString, setOriginalTagString] = useState('') // used to diff against the input
  const [tagString, setTagString] = useState('')
  const [tagsArray, setTagsArray] = React.useState<string[]>([])

  const [addTagResult, addTag] = useAppendTagByNameToDatapointsMutation()
  const [unTagResult, unTag] = useRemoveTagFromDatapointsMutation()

  const [datapoints, setDatapoints] = useAtom(datapointsAtom);
  const datapoint = datapoints[datapointId]

  useEffect(() => {
    var tagStrings = datapoint.tags.map(tag => tags[tag].name)
    setTagsArray(tagStrings)
    const allTags = tagStrings.join(", ")
    setTagString(allTags)
    setOriginalTagString(allTags)
  }, [tags])

  const checkAndSetName = (e: React.ChangeEvent<HTMLInputElement>, name: string) => {
    setTagString(e.currentTarget.value)
  }

  const onSubmitName = (e: React.FormEvent) => {
    e.preventDefault()
    setIsEditing(false)

    let newTagsArray = tagString.split(",").map(tag => tag.trim())
    if ((newTagsArray.length == 1) && (newTagsArray[0] == '')) newTagsArray = []
    let originalTagsArray = originalTagString.split(",").map(tag => tag.trim())
    if (tagString === originalTagString) return

    let remove = originalTagsArray.filter(x => !newTagsArray.includes(x)) // tags to remove
    let add = newTagsArray.filter(x => !originalTagsArray.includes(x)) // tags to add
    let keep = originalTagsArray.filter(x => newTagsArray.includes(x)) // tags to keep

    add.map(tagToAdd => {
      const variables = { tagName: tagToAdd, datapointIds: [datapointId] }
      addTag(variables)
    })

    remove.map(tagToRemove => {
      const variables = { tagName: tagToRemove, datapointIds: [datapointId] }
      unTag(variables)
    })

    var newTags = Object.assign({}, tags)
    var newDatapoints = Object.assign({}, datapoints)
    var dp = newDatapoints[datapointId]

    console.log('onSubmitName', add)

    // for every tag we want to add
    // 1. see if the tag already exists, if so, add its id to this list, and also add it to the tags list?
    // else create the tag, add it, and then add its id 
    var tempUUid = uuidv4()//Object.keys(newTags).length
    add.forEach(t => {
      var exists = Object.values(newTags).findIndex(existingTag => existingTag.name == t.trim()) // -1 means it doesnt exist yet, otherwise we need the index
      console.log('exists', exists)
      if (exists < 0) {
        // add and get the index
        tempUUid += 1
        newTags[tempUUid] = { id: tempUUid, name: t.trim(), datapoints: [dp.id] }
        dp.tags.push(tempUUid)
        console.log('wtf', tempUUid, newTags[tempUUid], dp.tags)
      } else {
        // add to the tag
        Object.values(newTags)[exists].datapoints.push(dp.id)
        // @ts-ignore
        dp.tags.push(Object.keys(newTags)[exists])
      }
    })

    var markForDeletion: number[] = []
    remove.map(t => {
      console.log('remove..', remove, t)
      if (t == '') return
      var exists = Object.values(newTags).findIndex(existingTag => existingTag.name == t.trim()) // -1 means it doesnt exist yet, otherwise we need the index
      var id = Object.values(newTags)[exists].id
      if (exists > -1) {
        removeItem(newTags[id].datapoints, dp.id)
        removeItem(dp.tags, id)
        if (newTags[id].datapoints.length === 0) {
          markForDeletion.push(id)
        }
      }
    })
    markForDeletion.map(deleteTagId => {
      delete newTags[deleteTagId]
    })

    console.log('newTags', newTags)

    setTags({ ...newTags })
    setDatapoints({ ...newDatapoints })
  }

  const onKeyPress = (e: any) => {
    if (e.key === 'Enter') {
      onSubmitName(e)
    }
    // I would like to catch ESC here, but it's getting caught elsewhere first.
  }

  // clicking out of the input resets it
  const handleBlur = (e: any) => {
    setIsEditing(false)
    setTagString(originalTagString)
  }

  let noTags = (tagsArray.length == 0)

  return (
    <>
      {isEditing ?
        <form onSubmit={onSubmitName} style={{ width: "100%" }}>
          <Textarea
            borderColor={"rgba(0,0,0,0)"}
            borderRadius={1}
            borderWidth={2}
            size="sm"
            p="7px"
            onKeyPress={onKeyPress}
            value={tagString}
            autoFocus={true}
            onChange={(e: any) => checkAndSetName(e, e.target.value)}
            _hover={{ borderColor: theme.colors.ch_gray.light }}
            _focus={{ borderColor: theme.colors.ch_blue }}
            onBlur={handleBlur}
            placeholder='Tags' />
        </form>
        :
        <Box
          onClick={() => setIsEditing(!isEditing)}
          width="100%"
          borderColor={"rgba(0,0,0,0)"}
          borderRadius={1}
          borderWidth={2}
          p={1}
          _hover={{ borderColor: theme.colors.ch_gray.light, cursor: 'pointer' }}
        >
          {tagsArray.map((tag) => {
            return (
              <TagButton key={tag} tag={tag} />
            )
          })}
          {noTags ?
            <Text fontSize='0.6em' >No tags</Text>
            : null}
        </Box>
      }
    </>
  )
}

export default Tags

// @ts-nocheck

import { EditIcon } from '@chakra-ui/icons'
import {
  FormControl,
  FormLabel,
  Input,
  Stack,
  ButtonGroup,
  Button,
  useDisclosure,
  Box,
  Popover,
  PopoverTrigger,
  IconButton,
  PopoverContent,
  PopoverArrow,
  PopoverCloseButton,
  InputGroup,
  InputLeftElement,
  CloseButton,
  useTheme
} from '@chakra-ui/react'
import React, { useState } from 'react'
import FocusLock from 'react-focus-lock'
import { render } from 'react-dom'
import { BsTagFill, BsTag } from 'react-icons/bs'
import { useAppendTagByNameToDatapointsMutation, useRemoveTagFromDatapointsMutation } from '../../graphql/graphql'

interface TagFormProps {
  selectedPoints: []
  serverData: []
  setServerData: () => void
}

const TagForm: React.FC<TagFormProps> = ({ selectedPoints, serverData, setServerData }) => {
  const theme = useTheme();
  const noneSelected = selectedPoints.length === 0

  const [newTag, setNewTag] = useState("")
  const [newUnTag, setNewUnTag] = useState("")

  const [addTagResult, addTag] = useAppendTagByNameToDatapointsMutation()
  const [unTagResult, unTag] = useRemoveTagFromDatapointsMutation()

  const onSubmitTagAll = (e: any) => {
    e.preventDefault()

    let splitNewTags = newTag.split(",")

    var selectedDatapointIds = selectedPoints.map(selectedProjection => {
      return serverData[selectedProjection].embedding.datapoint.id
    })

    splitNewTags.map(tag => {
      const variables = { tagName: tag, datapointIds: selectedDatapointIds };
      addTag(variables).then(result => {
      });
    })

    selectedPoints.forEach((point, index) => {
      var pointTags = serverData[point].embedding.datapoint.tags.slice()
      splitNewTags.forEach(splitNewTag => {
        const indexOf = pointTags.findIndex(currentTag => {
          return currentTag.tag.name === splitNewTag.trim();
        });

        if (indexOf < 0) {
          pointTags.push({ "right_id": null, "tag": { "name": splitNewTag.trim() } })
        }
      })
      serverData[point].embedding.datapoint.tags = pointTags
    })

    setServerData(serverData)

    setNewTag("")
  }

  const onSubmitUntagAll = (e: any) => {
    e.preventDefault()

    let splitNewUnTags = newUnTag.split(",")

    var selectedDatapointIds = selectedPoints.map(selectedProjection => {
      return serverData[selectedProjection].embedding.datapoint.id
    })

    splitNewUnTags.map(tag => {
      const variables = { tagName: tag, datapointIds: selectedDatapointIds };
      unTag(variables).then(result => {
      });
    })

    selectedPoints.forEach(point => {
      var tags = serverData[point].embedding.datapoint.tags.slice()

      splitNewUnTags.forEach(splitNewTag => {
        const indexOf = tags.findIndex(currentTag => {
          return currentTag.tag.name === splitNewTag.trim();
        });

        if (indexOf > -1) {
          tags.splice(indexOf, 1)
        }
      })

      serverData[point].embedding.datapoint.tags = tags
    })

    setServerData(serverData)

    setNewUnTag("")
  }

  const checkAndSetTag = (e: React.ChangeEvent<HTMLInputElement>, name: string) => {
    setNewTag(e.currentTarget.value)
  }

  const checkAndSetUnTag = (e: React.ChangeEvent<HTMLInputElement>, name: string) => {
    setNewUnTag(e.currentTarget.value)
  }

  const clearTag = () => {
    setNewTag("")
  }
  const clearUnTag = () => {
    setNewUnTag("")
  }

  return (
    <>
      <form onSubmit={onSubmitTagAll} style={{ width: "100%" }}>
        <InputGroup ml={3} mr={3} width="auto" pt={0}>
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
            placeholder='Untag selected' />
        </InputGroup>
      </form>
    </>
  )
}

export default TagForm

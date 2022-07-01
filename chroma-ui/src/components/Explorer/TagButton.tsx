
import React from 'react';
import {
  Tag,
} from '@chakra-ui/react'

interface TagButtonProps {
  tag: any
}

const TagButton: React.FC<TagButtonProps> = ({ tag }) => {
  return (
    <Tag key={tag} mr={1} mb={1} fontSize='0.6em' >{tag}</Tag>
  )
}

export default TagButton

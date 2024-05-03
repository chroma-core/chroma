// @ts-expect-error markdocs doesn't yet expose this properly
import { link as NextLink } from '@markdoc/next.js/tags';

import { AppLink } from '../../components/markdoc/AppLink';

export const link = {
  ...NextLink,
  render: AppLink
};

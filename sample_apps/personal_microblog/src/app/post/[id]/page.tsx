import { notFound } from "next/navigation";
import PermalinkTweetView from "@/components/permalink-tweet-view";
import { getPostById, getPostReplies, semanticSearch } from "@/actions";
import { TweetModel } from "@/types";

export default async function PostPermalinkPage({
  params,
}: {
  params: { id: string };
}) {
  const id = (await params).id;
  const post = await getPostById(id);

  if (!post) {
    notFound();
  }

  const [parentPosts, replies, relatedPosts] = await Promise.all([
    getParentPosts(post),
    getPostReplies(id),
    semanticSearch(post.body)
  ]);

  const relatedPostsFiltered = relatedPosts
    .filter(p => p.id !== post.id)
    .filter(p => !replies.some(r => r.id === p.id));

  return <PermalinkTweetView
    post={post}
    parentPosts={parentPosts}
    existingReplies={replies}
    relatedPosts={relatedPostsFiltered}
  />;
}

async function getParentPosts(post: TweetModel): Promise<TweetModel[]> {
  const parentPosts = [];
  let currentPost = post;
  while (currentPost.threadParentId) {
    const parentPost = await getPostById(currentPost.threadParentId);
    if (parentPost) {
      parentPosts.push(parentPost);
      currentPost = parentPost;
    } else {
      break;
    }
  }
  return parentPosts;
}

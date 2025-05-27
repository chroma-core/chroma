import { PostModel } from "./types";

function groupPostsByMonthAndYear(
  posts: PostModel[]
): { month: string; posts: PostModel[] }[] {
  const groupedPosts: { [key: string]: PostModel[] } = {};
  posts.forEach((post) => {
    const date = new Date(post.date);
    const monthYear = `${date.getMonth()}-${date.getFullYear()}`;
    if (!groupedPosts[monthYear]) {
      groupedPosts[monthYear] = [];
    }
    groupedPosts[monthYear].push(post);
  })

  const orderedPostGroups = Object.keys(groupedPosts).map((monthYear) => ({
    month: monthYear,
    posts: groupedPosts[monthYear],
  }))

  orderedPostGroups.sort((a, b) => a.month.localeCompare(b.month))

  return orderedPostGroups;
}

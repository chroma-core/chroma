import path from "path";
import fs from "fs";
import { OpenAI } from "openai";

const CONTENT_DIRECTORY = path.join(process.cwd(), "markdoc", "content");
const PUBLIC_DIRECTORY = path.join(process.cwd(), "public");
const LLMS_FILE = path.join(PUBLIC_DIRECTORY, "llms.txt");
const LLMS_FULL_FILE = path.join(PUBLIC_DIRECTORY, "llms-full.txt");

interface PageInfo {
  title: string;
  path: string;
  description: string;
}

const openai = new OpenAI();

const toTitle = (input: string): string => {
  return input
    .split("-")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join(" ");
};

const getPageInfo = async (
  content: string,
): Promise<{ title: string; description: string }> => {
  const systemPrompt =
    "You are a helpful assistant. The user will give you the contents of the page from the Chroma documentation. Your job is to provide a 1-sentence description of the content. Provide the description alone and nothing else. For example, if the page is an introduction to the Chroma Cloud section of the docs, your output may be: The page provides an introduction to Chroma Cloud, covering...";

  const userPrompt = `Pleas describe what the following page from the Chroma documentation covers: ${content}`;

  const chatCompletion = await openai.chat.completions.create({
    model: "gpt-4o",
    messages: [
      { role: "system", content: systemPrompt },
      {
        role: "user",
        content: userPrompt,
      },
    ],
  });

  const description = chatCompletion.choices[0].message?.content ?? "";

  const match = content.match(/^# (.+)$/m);
  const title = match ? match[1] : "";

  return { title, description };
};

const generateLLMsTxt = (pages: PageInfo[]) => {
  const pagesBySection = pages.reduce(
    (pages, page) => {
      const section = page.path.split("/")[1];
      pages[section] = [...(pages[section] || []), page];
      return pages;
    },
    {} as { [section: string]: PageInfo[] },
  );

  Object.entries(pagesBySection).forEach(([section, pages]) => {
    const pagesList = pages.map(
      (page) =>
        `- [${page.title}](https://docs.trychroma.com${page.path}): ${page.description}`,
    );
    const sectionContent = `# ${toTitle(section)}\n\n${pagesList.join("\n")}\n\n`;
    fs.appendFileSync(LLMS_FILE, sectionContent, "utf-8");
  });
};

const generateLLMFile = (filePath: string, content: string) => {
  let output = content;

  // Replace tabs with titles
  output = output.replace(
    /{% Tab label="([^"]+)" %}([\s\S]*?){% \/Tab %}/g,
    (_match, label, content) => {
      return `### ${label.trim()}\n\n${content.trim()}`;
    },
  );

  // Remove markdoc components
  output = output.replace(/{%[^%]*%}/g, "");

  // Clean up extra blank lines
  output = output.replace(/\n{3,}/g, "\n\n").trim();

  const publicPath = path.join(
    PUBLIC_DIRECTORY,
    `llms${filePath.replaceAll("/", "-")}`,
  );
  const dir = path.dirname(publicPath);
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(publicPath.replace(".md", ".txt"), output, "utf-8");
};

const walk = async (
  currentDir: string,
  docsContent: { fullContent: string; pages: PageInfo[] },
) => {
  const entries = fs.readdirSync(currentDir, { withFileTypes: true });

  for (const entry of entries) {
    const fullPath = path.join(currentDir, entry.name);
    if (entry.isDirectory()) {
      await walk(fullPath, docsContent);
    } else if (entry.isFile() && entry.name.endsWith(".md")) {
      const content = fs.readFileSync(fullPath, "utf-8");
      docsContent.fullContent += content;
      const contentPath = fullPath.replace(CONTENT_DIRECTORY, "");

      generateLLMFile(contentPath, content);
      const { title, description } = await getPageInfo(content);
      docsContent.pages.push({ title, description, path: contentPath });
    }
  }
};

const main = async () => {
  if (!process.env.OPENAI_API_KEY) {
    throw new Error("Missing OPENAI_API_KEY");
  }

  fs.rmSync(LLMS_FILE, { force: true });
  fs.rmSync(LLMS_FULL_FILE, { force: true });

  const docsContent = { fullContent: "", pages: [] };
  await walk(CONTENT_DIRECTORY, docsContent);

  const llmsFullPath = path.join(PUBLIC_DIRECTORY, "llms-full.text");
  fs.writeFileSync(llmsFullPath, docsContent.fullContent, "utf-8");

  generateLLMsTxt(docsContent.pages);
};

main().catch((err) => console.error(err));

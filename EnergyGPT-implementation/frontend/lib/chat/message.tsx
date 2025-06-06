'use client'

import { IconOpenAI, IconAnthropic, IconTool, IconUser } from '@/components/ui/icons'
import { cn } from '@/lib/utils'
import { spinner } from '@/components/stocks/spinner'
import { CodeBlock } from '@/components/ui/codeblock'
import { MemoizedReactMarkdown } from '@/components/markdown'
import remarkGfm from 'remark-gfm'
import remarkMath from 'remark-math'
import { StreamableValue, useStreamableValue } from 'ai/rsc'
import { useStreamableText } from '@/lib/hooks/use-streamable-text'
import CollapsibleUI from './clientComponents/collapsibleUI'
import './index.css';


function replaceXMLTags(text: string): string {
	// Tags to completely remove (with their content)
	const removeWithContent = ['count', 'thinking'];

  // First handle tags that should be removed with their content
	removeWithContent.forEach(tag => {
    if (tag === 'thinking') {
      // text = text.replace(new RegExp(`<${tag}>(.*?)</${tag}>`, 'gs'), `*Thoughts*`);
		  // text = text.replace(new RegExp(`<${tag}>(.*?)(?:</${tag}>|$)`, 'gs'), `*Thinking....*`);
    } else if (tag === 'count') {
      text = text.replace(new RegExp(`<${tag}.*?>(.*?)</${tag}>`, 'gs'), '');
    }
	});

  // Remove backslashed XML tags
	text = text.replace(new RegExp(`</([\\s\\S]*?)>`), '');
	
	// Handle tags that should be replaced with markdown bold
	const replaceWithBold = ['thinking', 'step', 'reflection', 'answer', 'observation', 'caption', 'evaluation', 'category', 'reward', 'report'];
	replaceWithBold.forEach(tag => {
		// Handle both single-line and multi-line content
		text = text.replace(new RegExp(`<${tag}.*?>(.*?)</${tag}>`, 'gs'), `**${tag}**: $1`);
		text = text.replace(new RegExp(`<${tag}.*?>(.*?)`, 'gs'), `**${tag}**: $1`);
	});

	// Handle tags that should be removed but keep their content
	const removeKeepContent: string[] = [];
	removeKeepContent.forEach(tag => {
		text = text.replace(new RegExp(`<${tag}.*?>(.*?)`, 'gs'), '$1');
	});

	return text;
}

function replaceJson(text: string): string {
  try {
    const json = JSON.parse(text);
    return Object.entries(json)
      .map(([key, value]) => ` - **${key}**: ${value}`)
      .join('\n');
  } catch {
    return text; // Return original content if JSON parsing fails
  }
}


// Different types of message bubbles.

export function UserMessage({ children }: { children: React.ReactNode }) {
  return (
    // <div className="group relative flex items-start md:-ml-12">
    //   <div className="flex size-[25px] shrink-0 select-none items-center justify-center rounded-md border bg-background shadow-sm">
    //     <IconUser />
    //   </div>
    //   <div className="ml-4 flex-1 space-y-2 overflow-hidden pl-2 text-muted-foreground">
    //     {children}
    //   </div>
    // </div>
    <div className="group-data-[role=user]/message:bg-primary group-data-[role=user]/message:text-primary-foreground flex gap-4 group-data-[role=user]/message:px-3 w-full group-data-[role=user]/message:w-fit group-data-[role=user]/message:ml-auto group-data-[role=user]/message:max-w-2xl group-data-[role=user]/message:py-2 rounded-xl">
      {children}
    </div>
  )
}

export function BotMessage({
  name,
  content,
  className,
  icon,
  agentName = "",
}: {
    name?: string
    content: string | StreamableValue<string>
    className?: string
    icon?: React.ReactNode,
    agentName?: string
}) {
  const text = replaceXMLTags(replaceJson(useStreamableText(content)))

  return (
    <div className={cn('group relative flex items-start md:-ml-12', className)}>
      <div className="flex shrink-0 select-none agent-name">
        {agentName}
      </div>
      <div className="flex size-[25px] shrink-0 select-none items-center justify-center border bg-background shadow-sm bot-icon">
        {icon}
      </div>
      <div className="ml-4 flex-1 space-y-2 overflow-hidden px-1">
        <MemoizedReactMarkdown
          className="prose break-words dark:prose-invert prose-p:leading-relaxed prose-pre:p-0 max-w-4xl"
          remarkPlugins={[remarkGfm, remarkMath]}
          components={{
            p({ children }) {
              return <p className="mb-2 last:mb-0">{children}</p>
            },

            code({ node, inline, className, children, ...props }) {
              if (children.length) {
                if (children[0] == '▍') {
                  return (
                    <span className="mt-1 animate-pulse cursor-default">▍</span>
                  )
                }
                children[0] = (children[0] as string).replace('`▍`', '▍')
              }

              const match = /language-(\w+)/.exec(className || '')

              if (inline) {
                return (
                  <code className={className} {...props}>
                    {children}
                  </code>
                )
              }

              return (
                <CodeBlock
                  key={Math.random()}
                  language={(match && match[1]) || ''}
                  value={String(children).replace(/\n$/, '')}
                  {...props}
                />
              )
            }
          }}
        >
          {text}
        </MemoizedReactMarkdown>
      </div>
    </div>
  )
}

export function BotCard({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <div className='human-message'>
      <div className="group relative flex items-start md:-ml-12 human-chat">
        <div className="flex-1">{children}</div>
      </div>
    </div>
  )
}

export function SystemMessage({ children }: { children: React.ReactNode }) {
  return (
    <div
      className={
        'mt-2 flex items-center justify-center gap-2 text-xs text-gray-500'
      }
    >
      <div className={'max-w-[600px] flex-initial p-2'}>{children}</div>
    </div>
  )
}

export function SpinnerMessage() {
  return (
    <div className="group relative flex items-start md:-ml-12">
      <div className="flex size-[24px] shrink-0 select-none items-center justify-center rounded-md border bg-primary text-primary-foreground shadow-sm">
        <IconOpenAI />
      </div>
      <div className="ml-4 h-[24px] flex flex-row items-center flex-1 space-y-2 overflow-hidden px-1">
        {spinner}
      </div>
    </div>
  )
}
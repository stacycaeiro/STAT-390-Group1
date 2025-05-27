import { UseChatHelpers } from 'ai/react'

import { Button } from '@/components/ui/button'
import { ExternalLink } from '@/components/external-link'
import { IconArrowRight } from '@/components/ui/icons'

export function EmptyScreen() {
  return (
    <div className="mx-auto max-w-2xl px-4">
      <div className="flex flex-col gap-2 rounded-lg border bg-background p-8">
        <h1 className="text-lg font-semibold">
          SciSciGPT
        </h1>
        <p className="leading-normal text-muted-foreground">
          This is an data analyst LLM agent build upon{' '}
          <ExternalLink href="https://www.nature.com/articles/s41597-023-02198-9">SciSciNet</ExternalLink>.
        </p>
        <p className="leading-normal text-muted-foreground">
          It uses Anthropic Claude 3.5 Sonnet, OpenAI GPT-4-turbo, LangChain and many other tools to provide data science & analysis assistance for Science of Science researchers.
        </p>
      </div>
    </div>
  )
}

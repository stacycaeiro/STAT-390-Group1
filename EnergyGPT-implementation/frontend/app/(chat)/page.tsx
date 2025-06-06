import { nanoid } from '@/lib/utils'
import { Chat } from '@/components/chat'
import { AI } from '@/lib/chat/actions'
import { auth } from '@/auth'
import { Session } from '@/lib/types'
import { getMissingKeys } from '@/app/actions'
import LoginForm from '@/components/login-form'

export const metadata = {
  title: 'SciSciGPT'
}

export default async function IndexPage() {
  const session = (await auth()) as Session
  
  if (!session) {
    return (
      <main className="flex flex-col items-center w-full min-h-screen">
        <div className="w-full max-w-md px-4 mt-8">
          <LoginForm />
        </div>
      </main>
    )
  } else {
    const id = nanoid()
    const missingKeys = await getMissingKeys()
    return (
      <AI initialAIState={{ chatId: id, messages: [] }}>
        <Chat id={id} session={session} missingKeys={missingKeys} />
      </AI>
    )
  }
}

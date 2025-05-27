import 'server-only'

export const maxDuration = 300;

import { createAI, createStreamableUI, getMutableAIState, getAIState, createStreamableValue } from 'ai/rsc'
import { Separator } from '@/components/ui/separator'
import { nanoid } from '@/lib/utils'
import { saveChat } from '@/app/actions'
import { Chat, Message } from '@/lib/types'
import { auth } from '@/auth'
import { StreamEvent } from '@langchain/core/dist/tracers/event_stream';
import { TokenReplacer } from '@/lib/chat/elements'

import { 
	render_tool_call_event, 
	render_tool_response_event, 
	render_user_message, 
	render_bot_message, render_bot_stream, 
	render_history_message 
} from '@/lib/chat/render'

import { RemoteRunnable } from "@langchain/core/runnables/remote";
const remoteChain = new RemoteRunnable({
	url: "http://localhost:8080/sciscigpt",
	options: { timeout: 3600000 } // 60 minutes
});

// CRITICAL FIX: Sanitization function for DataFrame tool outputs
function sanitizeToolOutput(output: any): any {
    try {
        // Handle DataFrame tool tuple responses
        if (Array.isArray(output) && output.length === 2 && 
            typeof output[0] === 'object' && typeof output[1] === 'object') {
            // DataFrame tools return (response, response) - use first element
            output = output[0];
        }
        
        // Ensure output is a plain object
        if (typeof output !== 'object' || output === null) {
            return { response: String(output) };
        }
        
        // Deep clone and sanitize the object
        const sanitized = JSON.parse(JSON.stringify(output, (key, value) => {
            // Replace any non-serializable values
            if (typeof value === 'function') {
                return '[Function]';
            }
            if (typeof value === 'undefined') {
                return null;
            }
            if (value instanceof Date) {
                return value.toISOString();
            }
            if (typeof value === 'bigint') {
                return value.toString();
            }
            if (typeof value === 'symbol') {
                return value.toString();
            }
            return value;
        }));
        
        return sanitized;
        
    } catch (error) {
        console.error('Error sanitizing tool output:', error);
        // Fallback: return a safe string representation
        return { 
            response: String(output),
            error: 'Output sanitization failed'
        };
    }
}

async function submitUserMessage(
	content: string, 
	fileList: any[] = [], 
	backendUrl: string = "http://localhost:8080/sciscigpt",
	if_inner_reasoning: boolean = false,
	if_multimodal: boolean = true,
) {
	'use server'

	const aiState = getMutableAIState<typeof AI>()

	const session_id = aiState.get().chatId
	const db_name = "SciSciNet_US_V4"

	const tool_config_by_name = {
		python:					{ session_id: session_id },
		r:					{ session_id: session_id },
		// DataFrame tools (new local tools)
		df_list_table:				{ query: "" },
		df_get_schema:				{ },
		df_query:				{ session_id: session_id },
		// Legacy SQL tools (if still used)
		sql_list_table:				{ db_name: db_name, query: "" },
		sql_get_schema:				{ db_name: db_name },
		sql_query:				{ db_name: db_name, session_id: session_id }, 
		// Other tools
		neo4j_get_schema:			{ },
		neo4j_query:				{ session_id: session_id },
		search_name:				{ },
		search_literature:			{ },
		search_policy:				{ },
		openalex_query:				{ session_id: session_id },
		arxiv_query:				{ session_id: session_id },
		google_search:				{ session_id: session_id }
	} // This defines the tool system arguments. Other parameters should be given by Agent.
	

	const human_message = {id: nanoid(), role: 'user', content: [
		{ type: 'text', text: content }, 
		...fileList.map(file => ({ type: 'image_url', image_url: { url: file } }))
	]}
	aiState.update({ ...aiState.get(), messages: [...aiState.get().messages, human_message] });

	const token_replacer = new TokenReplacer('sandbox:data/output', '/images')

	let textStream: undefined | ReturnType<typeof createStreamableValue<string>>
	let textNode: undefined | React.ReactNode
	let textContent: string = "";

	let tool_start_event: undefined | StreamEvent;
	let tool_end_event: undefined | StreamEvent;

	const streamableUI = createStreamableUI();

	(async () => {
		try {
			streamableUI.append(render_user_message(human_message));

			const messages = getMessagesFromAIState(aiState)
			const messages_str = JSON.stringify(messages.map(msg => msg.toJSON()), null, 4)
            console.log(messages_str)
			
			// STREAMING FIX: Better error handling for the event stream
			const eventStream = remoteChain.streamEvents(
				{ messages_str: messages_str, tool_config_by_name: tool_config_by_name }, 
				{ version: "v1" }
			); 

			for await (const event of eventStream) {
				const eventType = event.event;
				const metadata = event.metadata;
				
				// STREAMING FIX: Better handling of text streaming
				if (eventType === "on_chat_model_stream" || eventType === "on_llm_stream") {
					// For Anthropic
					// const delta = event.data.chunk?.content[0]?.text;
					// For OpenAI
					// const delta = event.data.chunk.content;

					const delta = event.data.chunk?.content?.[0]?.text ?? event.data.chunk?.content ?? '';

					if (delta !== undefined && delta !== "" && typeof delta === 'string') {
						if (textStream === undefined) {
							textStream = createStreamableValue<string>("");
							textNode = render_bot_stream(textStream.value, metadata);
							streamableUI.append(<Separator className="my-4" />);
							streamableUI.append(textNode);

							textContent = "";
						} 
						textStream.update(delta);
						textContent += delta;
					}
				} 
				// STREAMING FIX: Finalize text stream when moving to next event
				else if (textStream !== undefined) {
					const ai_message = {id: nanoid(), role: 'assistant', content: textContent, metadata: metadata};
					aiState.update({ ...aiState.get(), messages: [ ...aiState.get().messages, ai_message ] });
					
					textStream.done();
					textStream = undefined;
					textContent = "";
				} // One token at a time
				
				if (eventType === "on_tool_start") {
					// STREAMING FIX: Ensure text stream is closed before tool events
					if (textStream !== undefined) {
						const ai_message = {id: nanoid(), role: 'assistant', content: textContent, metadata: metadata};
						aiState.update({ ...aiState.get(), messages: [ ...aiState.get().messages, ai_message ] });
						textStream.done();
						textStream = undefined;
						textContent = "";
					}
					
					tool_start_event = event;
					
					streamableUI.append(<Separator className="my-4" />);
					streamableUI.append(render_tool_call_event(tool_start_event));
				} // One tool call at a time
				
				if (eventType === "on_tool_end") {
					tool_end_event = event;
		
					const tool_name = tool_end_event.name;
					const tool_call_id = tool_end_event.run_id;
					
					// CRITICAL FIX: Properly handle DataFrame tool outputs
					let raw_output = typeof tool_end_event.data.output === "string" 
						? JSON.parse(tool_end_event.data.output) 
						: tool_end_event.data.output;
					
					// Sanitize the output to ensure it's serializable for React
					const tool_output = sanitizeToolOutput(raw_output);
					
					var toolArgs = {};
					if (tool_start_event !== undefined) {
						toolArgs = typeof tool_start_event.data.input === "string" 
							? JSON.parse(tool_start_event.data.input) 
							: tool_start_event.data.input;
					}
		
					const tool_call = { 
						id: nanoid(), role: 'assistant',
						content: [{ type: 'tool-call', tool_name: tool_name, tool_call_id: tool_call_id, args: toolArgs }]
					}
					aiState.update({ ...aiState.get(), messages: [...aiState.get().messages, tool_call] });

					const tool_response = {
						id: nanoid(), role: 'tool',
						content: [{ type: 'tool-result', tool_name: tool_name, tool_call_id: tool_call_id, result: tool_output }]
					}
					aiState.update({ ...aiState.get(), messages: [...aiState.get().messages, tool_response] });

					streamableUI.append(<Separator className="my-4" />);
					
					// STREAMING FIX: Create a sanitized event for rendering
					const sanitizedEvent = {
						...tool_end_event,
						data: {
							...tool_end_event.data,
							output: tool_output
						}
					};
					streamableUI.append(render_tool_response_event(sanitizedEvent));
				} // One tool response at a time
			}
			
			// STREAMING FIX: Ensure final text stream is properly closed
			if (textStream !== undefined) {
				const ai_message = {id: nanoid(), role: 'assistant', content: textContent, metadata: {}};
				aiState.update({ ...aiState.get(), messages: [ ...aiState.get().messages, ai_message ] });
				textStream.done();
				textStream = undefined;
			}
			
		} catch (e: any) {
			console.error('Error in submitUserMessage:', e)
			streamableUI.append(<Separator className="my-4" />);
			streamableUI.append("An error occurred. Please try again.\n\n" + e.toString());
		} finally {
			// STREAMING FIX: Ensure all streams are properly closed
			try {
				aiState.done(aiState.get())
				streamableUI.done()
				if (textStream !== undefined) {
					textStream.done()
				}
			} catch (finalError) {
				console.error('Error in finally block:', finalError);
			}
		}
	})()
	
	return {
		id: nanoid(),
		display: streamableUI.value
	}
}

export type AIState = {
	chatId: string
	messages: any[]
}

export type UIState = {
	id: string
	display: React.ReactNode
}[]

export const AI = createAI<AIState, UIState>({
	actions: {
		submitUserMessage
	},
	initialUIState: [],
	initialAIState: { chatId: nanoid(), messages: [] },
	onGetUIState: async () => { 
		'use server'

		const session = await auth()

		if (session && session.user) {
			const aiState = getAIState() as Chat

			if (aiState) {
				const uiState = getUIStateFromAIState(aiState)
				return uiState
			}
		} else {
			return
		}
	},
	onSetAIState: async ({ state }) => {
		'use server'

		const session = await auth()

		if (session && session.user) {

			const { chatId, messages } = state

			const createdAt = new Date()
			const userId = session.user.id as string
			const path = `/chat/${chatId}`

			const firstMessageContent = messages[0].content[0].text as string
			const title = firstMessageContent.substring(0, 100)

			const chat: Chat = {
				id: chatId,
				title,
				userId,
				createdAt,
				messages,
				path
			}

			await saveChat(chat)

		} else {
			return
		}
	}
})

export const getUIStateFromAIState = (aiState: any) => {
	return aiState.messages
		.filter((message: any) => message.role !== 'system')
		.map((message: any, index: any) => ({
			id: `${aiState.chatId}-${index}`,
			display: render_history_message(message)
		}))
}

export function getMessagesFromAIState(aiState: any) {
	return getMessagesFromAIStates(aiState.get().messages)
}

import { AIMessage, BaseMessage, HumanMessage, ToolMessage } from "@langchain/core/messages";

function getMessageFromAIState(msg: any) {
	const role = msg.role;

	if (role === 'user') {
		const message = new HumanMessage({ content: msg.content })
		return message

	} else if (role === 'assistant') { 
		if (typeof msg.content === 'string') { // AI response 
			const message = new AIMessage({ content: msg.content })
			return message

		} else { // AI tool call 
			const message = new AIMessage({
					content: "",
					tool_calls: msg.content.map((
							content: { type: string, tool_name: string, tool_call_id: string, args: any }
					) => ({ id: content.tool_call_id, name: content.tool_name, args: content.args })),
			});
			return message
		}
	} else if (role === 'tool') { // Tool Response
		// CRITICAL FIX: Sanitize tool result before creating ToolMessage
		let result = msg.content[0].result;
		
		// Handle DataFrame tool responses and ensure serializable content
		result = sanitizeToolOutput(result);
		
		const message = new ToolMessage({
			content: JSON.stringify(result),
			tool_call_id: msg.content[0].tool_call_id,
		})
		return message
	} else {
		const message = null
		return message
	}
}

function getMessagesFromAIStates(msgs: any[]) {
	const mergedMessages: BaseMessage[] = [];
	
	for (let i = 0; i < msgs.length; i++) {
		const msg = msgs[i];
		const role = msg.role;

		if (role === 'user') {
			const message = new HumanMessage({ content: msg.content });
			mergedMessages.push(message);
		} else if (role === 'assistant') {
			if (typeof msg.content === 'string') { // AI response 
				if (i + 1 < msgs.length && msgs[i + 1].role === 'assistant' && typeof msgs[i + 1].content !== 'string') {
					// Merge this message with the next one
					const toolCalls = msgs[i + 1].content.map((content: { type: string, tool_name: string, tool_call_id: string, args: any }) => ({
						id: content.tool_call_id,
						name: content.tool_name,
						args: content.args
					}));

					const message = new AIMessage({
						content: msg.content,
						tool_calls: toolCalls
					});
					mergedMessages.push(message);
					i++; // Skip the next message as it has been merged
				} else {
					const message = new AIMessage({ content: msg.content });
					mergedMessages.push(message);
				}
			} else { // AI tool call without content
				const message = new AIMessage({
					content: "",
					tool_calls: msg.content.map((content: { type: string, tool_name: string, tool_call_id: string, args: any }) => ({
						id: content.tool_call_id,
						name: content.tool_name,
						args: content.args
					}))
				});
				mergedMessages.push(message);
			}
		} else if (role === 'tool') { // Tool Response
			// CRITICAL FIX: Sanitize tool result before creating ToolMessage
			let result = msg.content[0].result;
			result = sanitizeToolOutput(result);
			
			const message = new ToolMessage({
				content: JSON.stringify(result),
				tool_call_id: msg.content[0].tool_call_id,
			});
			mergedMessages.push(message);
		} else {
			// Handle any unknown role or message type
			const message = null;
			if (message) mergedMessages.push(message);
		}
	}

	return mergedMessages;
}
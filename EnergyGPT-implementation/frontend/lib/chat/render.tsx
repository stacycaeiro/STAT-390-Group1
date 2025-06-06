import { StreamEvent } from '@langchain/core/dist/tracers/event_stream';
import { BotCard, UserMessage, BotMessage, SystemMessage, SpinnerMessage } from '@/lib/chat/message'
import { Separator } from '@/components/ui/separator'
import { ImageFromSrc } from '@/lib/chat/elements'
import ImageClientWrapper  from './clientComponents/imageLoader'
import { IconOpenAI, IconAnthropic, IconTool, IconUser } from '@/components/ui/icons'
import { llmName } from '@/llm.config';
import CSVDownLoadClientWrapper from './clientComponents/csvDownloader'
import './index.css';
import { resourceLimits } from 'worker_threads';

let aiIcon: React.ReactNode = <IconOpenAI />;
if (llmName as string === "OpenAI") {
	aiIcon = <IconOpenAI />;
} else {
	aiIcon = <IconAnthropic />;
}

// CRITICAL FIX: Sanitization function for event outputs
function sanitizeEventOutput(output: any): any {
	try {
		// Handle tuple responses from DataFrame tools
		if (Array.isArray(output) && output.length === 2) {
			output = output[0];
		}
		
		// If output has a 'response' field, prioritize that
		if (output && typeof output === 'object' && 'response' in output) {
			return {
				response: output.response,
				files: output.files || [],
				images: output.images || [],
				note: output.note || null
			};
		}
		
		// Ensure it's a plain object
		if (typeof output !== 'object' || output === null) {
			return { response: String(output) };
		}
		
		// Deep sanitize to remove any non-serializable content
		const sanitized = JSON.parse(JSON.stringify(output, (key, value) => {
			if (typeof value === 'function') return '[Function]';
			if (typeof value === 'undefined') return null;
			if (value instanceof Date) return value.toISOString();
			if (typeof value === 'bigint') return value.toString();
			if (typeof value === 'symbol') return value.toString();
			return value;
		}));
		
		return sanitized;
		
	} catch (error) {
		return { 
			response: 'Error processing tool response',
			error: error instanceof Error ? error.message : String(error)
		};
	}
}

export function render_tool_call_event(event: StreamEvent) {
	const name = event.name
	const args = typeof event.data.input === 'string' ? JSON.parse(event.data.input) : event.data.input
	const textResponse = __render_tool_call__(name, args)
	return (<div> {textResponse} </div>)
}

export function render_tool_call_message(message: any) {
	const name = message.content[0].tool_name
	const args = message.content[0].args
	const textResponse = __render_tool_call__(name, args)
	return (<div> {textResponse} </div>)
}

function __render_tool_call__(name: string, args: any) {
	var content = ''

	if (name.endsWith('specialist')) {
		content = `**Task -> ${name}:** \n\n${args.task}`
	} else

	// Handle both DataFrame and SQL query tools
	if (name === 'df_query' || name === 'sql_query' || name === 'neo4j_query' ) {
		content = '```sql\n' + args.query + '\n```'
	} else

	if (name === 'python') {
		content = '```python\n' + args.query + '\n```'
	} else
	
	if (name === 'r') {
		content = '```r\n' + args.query + '\n```'
	} else 
	
	{
		content = `Invoking tool: \`${name}\` with inputs: \`${JSON.stringify(args)}\``
	}

	const message: React.ReactNode = (
		<BotMessage content={ content } icon={ <IconTool/> } agentName={name}/>
	)

	return message
}

export function render_tool_response_event(event: StreamEvent) {
	// CRITICAL FIX: Sanitize the output before processing
	let output = typeof event.data.output === 'string' ? JSON.parse(event.data.output) : event.data.output;
	
	// Handle DataFrame tool responses that might contain complex objects
	output = sanitizeEventOutput(output);
	
	console.log('render_tool_response_event', event.name, output)
	return __render_tool_response__(event.name, output)
}

export function render_tool_response_message(message: any) {
	const name = message.content[0].tool_name as string;
	let result = message.content[0].result;
	
	// CRITICAL FIX: Sanitize the result before processing
	result = sanitizeEventOutput(result);
	
	return __render_tool_response__(name, result)
}

function __render_tool_response__ (name: string, result: any) {
	const text = result.response !== undefined ? result.response : ''
	const images = result.images !== undefined && result.images.length > 0 ? result.images: []

	const files = []
	if (result.file !== undefined) {
		files.push({
			name: result.file.split('/').pop(),
			id: result.file.split('/').pop().split('.')[0],
			download_link: result.file.replace("data/output/", "http://localhost:8080/output/"),
			mimeType: result.file.split('.').pop()
		})
	}
	if (result.files !== undefined) {
		files.push(...result.files)
	}
	
	const textResponse = __render_tool_response_text__(name, text)
	const imagesResponse = images.length > 0 ? <ImageClientWrapper images={images} /> : null
	const filesResponse = files.length > 0 ? files.map((file: any, index: number) => (
		<CSVDownLoadClientWrapper key={index} src={file.download_link} name={file.name} />
	)) : null

	return (
		<div>
			{textResponse}
			{imagesResponse}
			{filesResponse}
		</div>
	)
}

function __render_tool_response_text__ (name: string, text: string) {
	var content = '';
	
	// Handle DataFrame tools (new local tools) and legacy SQL tools
	if (name === 'df_list_table' || name === 'sql_list_table') {	
		content = '```\n' + text + '\n```'
	} else if (name === 'df_get_schema' || name === 'sql_get_schema') {	
		content = '```sql\n' + text + '\n```'
	} else if (name === 'df_query' || name === 'sql_query') {
		content = text ? "```\n" + text + "\n```" : ""
	} else if (name === 'neo4j_get_schema') {
		content = '```\n' + text + '\n```'
	} else if (name === 'neo4j_query') {
		content = text ? "```cypher\n" + text + "\n```" : ""
	} else if (name === 'python') {
		content = text ? "```python\n" + text + "\n```" : ""
	} else if (name === 'r') {
		content = text ? "```r\n" + text + "\n```" : ""
	} else if (name === 'search_policy' || name === 'search_policy_advanced' || name === 'search_literature') {
		// Policy/literature search results - render as markdown
		content = text
	} else if (name === 'search_name') {
		// Name search results
		content = text
	} else if (name.endsWith('_specialist')) {
		// Specialist responses
		content = text
	} else {
		// Default: render as plain text
		content = text
	}

	if (content === '') {
		return null
	} else {
		const message: React.ReactNode = (
			<BotMessage name={name} content={content} icon={ <IconTool/> }/>
		)
		return message
	}
}

export function render_user_message(message: any) {
	const image_nodes = message.content.filter((message: any) => message.type === 'image_url').map((message: any, index: any) => {
		const url = message.image_url.url ? message.image_url.url : message.image_url
		return ImageFromSrc(url, index)
	});

	const text_nodes = message.content.filter((message: any) => message.type === 'text').map((message: any, index: any) => {
		return message.text
	});

	return (
		<BotCard>
			{ text_nodes }
			{ image_nodes }
		</BotCard>
	)
}

export function render_bot_stream(content: any, metadata: any={}) {
	const agentName = metadata?.langgraph_node?.split('_').map((word: string) => word.charAt(0).toUpperCase() + word.slice(1)).join('');
	return <BotMessage content={content as string} icon={aiIcon} agentName={agentName}/>;
}

export function render_bot_message(message: any, metadata: any={}) {
	const content = message.content;
	const agentName = metadata?.langgraph_node?.split('_').map((word: string) => word.charAt(0).toUpperCase() + word.slice(1)).join('');
	return <BotMessage content={content as string} icon={aiIcon} agentName={agentName}/>;
}

export function render_history_message(message: any) {
	const role = message.role;
	if (role === 'user') {
		// Human input
		return render_user_message(message)
	} else if (role === 'assistant') { 
		if (typeof message.content === 'string') {
			// AI response 
			return render_bot_message(message, message.metadata)
		} else {
			// AI tool call 
			return render_tool_call_message(message)
		}
	} else {
		// Tool Response
		return render_tool_response_message(message)
	}
}
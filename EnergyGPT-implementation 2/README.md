# SciSciGPT

SciSciGPT is a multi-agent AI system designed to serve as a research collaborator for science of science researchers and practitioners.

## Overview

Drawing inspiration from the core research tasks of domain researchers, SciSciGPT functions as a team of five AI agents, each dedicated to a distinct aspect of the research process:

- **The ResearchManager agent** functions as a project leader and central coordinator. It orchestrates the research workflow, breaking complex research questions down into tasks and assigning them to the four specialist agents listed below.

- **The LiteratureSpecialist agent** focuses on comprehension and synthesis, searching for and organizing relevant information from the SciSci literature.

- **The DatabaseSpecialist agent** handles processing tasks, managing complex data extraction, transformation, and basic statistics across scholarly databases. This agent is equipped to interact with a comprehensive scholarly data repository.

- **The AnalyticsSpecialist agent** focuses on statistical analysis and modeling, implementing empirical methods and analytical techniques and generating visualizations to support empirical investigations.

- **The EvaluationSpecialist agent** assesses the quality, relevance, and rigor of SciSciGPT's analyses, visualizations, and methodological choices, allowing the system to identify potential improvements and adjust its approach iteratively.

## How It Works

When the ResearchManager receives a research question, it formulates an execution plan, assigning tasks to appropriate specialists. Each specialist agent formulates sub-plans, invokes tool use, and engages in iterative reasoning until the task is completed. As each plan is executed, the EvaluationSpecialist is invoked to assess progress across multiple levels, guiding the specialist's next step.

After the specialist finishes each task, the control returns to the ResearchManager for subsequent task allocation and execution. This hierarchical structure supports flexible task decomposition and delegation for any user question, enabling SciSci researchers to interact seamlessly with the system through conversation, refine their research questions, and explore different approaches as needed.

This conversational, multi-agent architecture enables domain-specific functionalities while maintaining the original LLM's general capability, such as instruction following, question answering, and common sense reasoning.

## Project Structure

- **Frontend**: Next.js-based user interface adapted from [Vercel's AI chatbot template](https://github.com/vercel/ai-chatbot)
- **Backend**: LangChain-powered agent system with specialized tools for scientific literature analysis

## Setup

Refer to the README files in the frontend and backend directories for specific setup instructions.

## Acknowledgements

- Frontend adapted from [Vercel AI Chatbot](https://github.com/vercel/ai-chatbot)
- Backend built with [LangChain](https://github.com/langchain-ai/langchain)

## Contact

Erzhuo Shao (shaoerzhuo@gmail.com)  
The Center for Science of Science & Innovation (CSSI)
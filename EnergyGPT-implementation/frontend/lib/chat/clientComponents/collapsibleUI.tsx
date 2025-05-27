"use client";

import React, { useState, useEffect } from "react";
import { Collapse } from "antd";
import './index.css';

const { Panel } = Collapse;

interface CollapsibleUIProps {
    shouldCollapse: boolean;
    children: React.ReactNode;
    status?: 'thinking' | 'thought';
}

const CollapsibleUI: React.FC<CollapsibleUIProps> = ({ shouldCollapse, children, status = 'thinking' }) => {
    // const [items, setItems] = useState<{ key: string; title: string; content: React.ReactNode }[]>(['']);

    return (
        <Collapse ghost style={{ width: '100%' }} className="collapse-container" defaultActiveKey={shouldCollapse ? [] : ["1"]}> 
            <Panel
                header={status == 'thinking' ? <span className="shimmer">Thinking</span> : ''} // Apply shimmer effect
                key="1"
            >
                {children}
            </Panel>
        </Collapse>
    );
};

export default CollapsibleUI;
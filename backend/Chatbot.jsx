import React, { useState, useRef, useEffect } from 'react';
// If using Axios, uncomment the next line:
// import axios from 'axios';

const API_URL = 'http://localhost:8000/chat';
const USER_ID = 1; // You can make this dynamic as needed

const Chatbot = () => {
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState([]); // { sender: 'user'|'bot', text: string }
  const chatEndRef = useRef(null);

  useEffect(() => {
    // Scroll to bottom when messages change
    if (chatEndRef.current) {
      chatEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages]);

  const sendMessage = async (e) => {
    e.preventDefault();
    if (!input.trim()) return;
    const userMessage = { sender: 'user', text: input };
    setMessages((msgs) => [...msgs, userMessage]);
    setInput('');
    try {
      // Using fetch (can use axios if preferred)
      const res = await fetch(API_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user_id: USER_ID, message: input })
      });
      const data = await res.json();
      setMessages((msgs) => [
        ...msgs,
        { sender: 'bot', text: data.response }
      ]);
    } catch (err) {
      setMessages((msgs) => [
        ...msgs,
        { sender: 'bot', text: "Sorry, I couldn't reach the server." }
      ]);
    }
  };

  return (
    <div className="max-w-md mx-auto mt-8 border rounded shadow-lg flex flex-col h-[500px] bg-white">
      <div className="flex-1 overflow-y-auto p-4" style={{ background: '#f9f9f9' }}>
        <ul className="space-y-2">
          {messages.map((msg, idx) => (
            <li key={idx} className={msg.sender === 'user' ? 'text-right' : 'text-left'}>
              <span className={
                msg.sender === 'user'
                  ? 'inline-block bg-blue-500 text-white px-3 py-2 rounded-lg'
                  : 'inline-block bg-gray-200 text-gray-800 px-3 py-2 rounded-lg'
              }>
                {msg.text}
              </span>
            </li>
          ))}
          <div ref={chatEndRef} />
        </ul>
      </div>
      <form onSubmit={sendMessage} className="p-4 border-t flex gap-2 bg-white">
        <input
          type="text"
          className="flex-1 border rounded px-3 py-2 focus:outline-none focus:ring"
          placeholder="Type your message..."
          value={input}
          onChange={e => setInput(e.target.value)}
        />
        <button
          type="submit"
          className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600"
        >
          Send
        </button>
      </form>
    </div>
  );
};

export default Chatbot; 
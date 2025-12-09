// Example: Connect to progress WebSocket
const ws = new WebSocket('ws://localhost:8000/ws/progress');

ws.onopen = () => {
    console.log('Connected to progress stream');
    
    // Start pipeline
    fetch('http://localhost:8000/pipeline/run-async', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
            steps: ['extract_csv', 'normalize_csv', 'load_neo4j'],
            args: {
                input_file: 'data/input.csv',
                output_file: 'data/output.csv',
                neo4j_uri: 'bolt://localhost:7687'
            }
        })
    })
    .then(r => r.json())
    .then(data => console.log('Pipeline started:', data.pipeline_id));
};

ws.onmessage = (event) => {
    const progressEvent = JSON.parse(event.data);
    
    console.log('Event:', progressEvent.event_type);
    console.log('Step:', progressEvent.step_name);
    
    if (progressEvent.progress) {
        console.log(`Progress: ${progressEvent.progress.percentage}%`);
        console.log(`Message: ${progressEvent.progress.message}`);
        
        // Update UI
        updateProgressBar(progressEvent.progress.percentage);
        updateStatusMessage(progressEvent.progress.message);
    }
};

ws.onerror = (error) => console.error('WebSocket error:', error);
ws.onclose = () => console.log('Disconnected from progress stream');
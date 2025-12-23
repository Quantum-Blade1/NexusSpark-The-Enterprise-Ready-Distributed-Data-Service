import React, { useState } from 'react';
import axios from 'axios';
import { useDropzone } from 'react-dropzone';
import { CircularProgressbar, buildStyles } from 'react-circular-progressbar';
import 'react-circular-progressbar/dist/styles.css';
import './App.css';

function App() {
    const [file, setFile] = useState(null);
    const [uploadProgress, setUploadProgress] = useState(0);
    const [isUploading, setIsUploading] = useState(false);
    const [downloadLink, setDownloadLink] = useState(null);

    const onDrop = (acceptedFiles) => {
        setFile(acceptedFiles[0]);
        console.log('File selected:', acceptedFiles[0]);
    };

    const { getRootProps, getInputProps } = useDropzone({ onDrop });

    const handleUpload = async () => {
        const formData = new FormData();
        formData.append('file', file);
        setIsUploading(true);

        try {
            const response = await axios.post('http://172.1.44.73:5000/upload', formData, {
                headers: {
                    'Content-Type': 'multipart/form-data',
                },
                onUploadProgress: (progressEvent) => {
                    const progress = Math.round((progressEvent.loaded * 100) / progressEvent.total);
                    setUploadProgress(progress);
                    console.log('Upload progress:', progress);
                }
            });
            console.log('Response from server:', response);
            setDownloadLink(response.data.download_link);
        } catch (error) {
            console.error('Error uploading file:', error);
        } finally {
            setIsUploading(false);
        }
    };

    return (
        <div className="App">
            <h1>Spark as a Service: Upload CSV File</h1>
            <div {...getRootProps({ className: 'dropzone' })}>
                <input {...getInputProps()} />
                <p>Drag & drop a file here, or click to select a file</p>
            </div>
            {file && (
                <div>
                    <p>Selected file: {file.name}</p>
                    <button onClick={handleUpload}>Upload</button>
                </div>
            )}
            {isUploading && (
                <div style={{ width: 100, height: 100, margin: '20px auto' }}>
                    <CircularProgressbar
                        value={uploadProgress}
                        text={`${uploadProgress}%`}
                        styles={buildStyles({
                            textColor: '#fff',
                            pathColor: '#4caf50',
                            trailColor: '#d6d6d6',
                        })}
                    />
                </div>
            )}
            {downloadLink && (
                <div>
                    <h2>Download Result</h2>
                    <a href={downloadLink}>Download result.csv</a>
                </div>
            )}
        </div>
    );
}

export default App;

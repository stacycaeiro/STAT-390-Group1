"use client"; 

import React, { useState } from "react";
import { Button } from "antd";
import { DownloadOutlined } from "@ant-design/icons";
import Image from "next/image";

export const ImageFromSrc = ({ imageBase64, name="image", alt = "Image" }: { imageBase64: string; name?: string; alt?: string }) => {
    const [hovered, setHovered] = useState(false);
    
    const handleDownload = async (event: React.MouseEvent<HTMLButtonElement>) => {
        event.preventDefault();
        event.stopPropagation();
    
        try {
            console.log("Downloading:", imageBase64); // Debugging: Check the URL
    
            const response = await fetch(imageBase64);
            console.log("Response", response);
            console.log("Response headers:", response.headers);
    
            const blob = await response.blob();
            console.log("Blob size:", blob.size, "Blob type:", blob.type);
    
            if (blob.size === 0) {
                throw new Error("Downloaded file is empty");
            }
    
            const mimeType = blob.type || "image/png"; // Fallback to PNG
    
            const blobUrl = URL.createObjectURL(new Blob([blob], { type: mimeType }));
    
            const link = document.createElement("a");
            link.href = blobUrl;
            link.download = name; // Force correct file extension
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
    
            URL.revokeObjectURL(blobUrl);
    
        } catch (error) {
            console.error("Error downloading the image:", error);
        }
    };
    
    console.log("Rendering image with URL:", imageBase64); // Debug log
  
    return (
      <div 
        style={{ position: "relative", display: "inline-block" }}
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
      >
        {(hovered || document.activeElement?.classList.contains("download-button")) && (
            <Button 
                type="text" 
                icon={<DownloadOutlined style={{ color: "white" }} />} 
                onClick={handleDownload} 
                style={{
                    position: "absolute",
                    top: 10,
                    left: 10,
                    zIndex: 10,
                    backgroundColor:"rgba(100, 100, 100, 0.6)",
                    borderRadius: "8px",
                    padding: "8px",
                    cursor: "pointer",
                    boxShadow: "0 4px 4px rgba(0, 0, 0, 0.1)",
                }}
            />
        )}
        <div style={{ position: "relative", width: "100%", height: "auto" }}>
          <Image
            src={imageBase64}
            alt={alt} 
            width={800}
            height={600}
            style={{ maxWidth: "100%", height: "auto" }}
            unoptimized={imageBase64.startsWith("http://localhost:8080")}
            onError={(e) => {
              console.error("Image failed to load:", e);
              console.log("Failed image URL:", imageBase64);
            }}
          />
        </div>
      </div>
    );
};
  
  
// export const ImageFromSrcs = ({ imageBase64s }: { imageBase64s: string[] }) => {
//     return (
//       <div>
//         {imageBase64s.map((imageBase64, index) => (
//             <ImageFromSrc key={index} imageBase64={imageBase64} />
//         ))}
//       </div>
//     )
// };

export default function ImageClientWrapper({ images }: { images: any[] }) {
    console.log("Received images:", images); // Debug log
    return (
        <div>
            {images.map((image, index) => (
                <ImageFromSrc 
                    key={index} 
                    imageBase64={image.download_link} 
                    name={image.name}
                />
            ))}
        </div>
    );
}
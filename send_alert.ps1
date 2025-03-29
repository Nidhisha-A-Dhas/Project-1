param (
    [string]$Message = "CRITICAL ALERT: Fire detected in your video. Immediate action required!",
    [string]$Subject = "Critical Event Detected"
)

aws sns publish `
--topic-arn "arn:aws:sns:ap-south-1:148831181875:RekognitionResults" `
--message "$Message" `
--subject "$Subject"

import React, {useState, useEffect} from "react"
import logo from "./images/logo.png";
import axios from "axios"
import "./stylesheets/App.css";

const UPDATE_FREQUENCY_MS = 2000

function App() {
  let date = new Date()
  let year = date.getFullYear()
  let month = date.getMonth()
  let day = date.getDate()
  let hour = date.getHours()
  let minute = date.getMinutes()
  let second = date.getSeconds()
  let timestamp = `${year}-${month}-${day}T${hour}:${minute}:${second}`

  const defaultStatistic = {
    num_immediate_requests: 0,
    num_scheduled_requests: 0,
    total_requests: 0.0,
    updated_timestamp: `${timestamp}Z`
  }

  const defaultCommonImmediateResidentialType = {
    most_common_immediate_residential_request: ""
  }

  const defaultCommonScheduledResidentialType = {
    most_common_scheduled_residential_request: ""
  }

  const [statistic, setStatistic] = useState(defaultStatistic)
  const [commonImmediateResidence, setCommonImmediateResidence] = useState(defaultCommonImmediateResidentialType)
  const [commonScheduledResidence, setCommonScheduledResidence] = useState(defaultCommonScheduledResidentialType)


  const getStatistic = () => {
    axios.get("http://ms-vm.westus.cloudapp.azure.com:8100/events/stats")
    .then(json => {
      console.log(json.data)
      setStatistic(json.data)
    })
  }

  const getCommonImmediateResidentialType = () => {
    axios.get("http://ms-vm.westus.cloudapp.azure.com:8110/rental/immediate/common-residence")
    .then(json => {
      console.log(json.data)
      setCommonImmediateResidence(json.data)
    })
  }

  const getCommonScheduledResidentialType = () => {
    axios.get(`http://ms-vm.westus.cloudapp.azure.com:8110/rental/schedule/common-residence?startDate=2020-06-17T17:32:59.283Z&endDate=2020-06-17T17:32:59.283Z`)
    .then(json => {
      console.log(json.data)
      setCommonScheduledResidence(json.data)
    })
  }

  useEffect(() => {
    setTimeout(getStatistic, UPDATE_FREQUENCY_MS)
  }, [statistic])

  useEffect(() => {
    setTimeout(getCommonImmediateResidentialType, UPDATE_FREQUENCY_MS)
  }, [commonImmediateResidence])

  useEffect(() => {
    setTimeout(getCommonScheduledResidentialType, UPDATE_FREQUENCY_MS)
  }, [commonScheduledResidence])

  return (
    <div className="app">
      <img src={logo} className="app-logo" alt="logo" />
      <h3># of immediate residential requests</h3>
      <p>{statistic.num_immediate_requests}</p>
      <h3># of scheduled residential requests</h3>
      <p>{statistic.num_scheduled_requests}</p>
      <h3>Total number of requests</h3>
      <p>{statistic.total_requests}</p>
      <h3>Last Updated</h3>
      <p>{statistic.updated_timestamp}</p>
      <h3>Most Common Immediate Request Overall</h3>
      <p>{commonImmediateResidence.most_common_immediate_residential_request}</p>
      <h3>Most Common Scheduled Request On June 17, 2020 @ 11:29PM</h3>
      <p>{commonScheduledResidence.most_common_scheduled_residential_request}</p>
    </div>
  );
}

export default App;

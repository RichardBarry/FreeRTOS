/*
 * FreeRTOS V202212.00
 * Copyright (C) 2020 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * https://www.FreeRTOS.org
 * https://github.com/FreeRTOS
 *
 */

/*
 * This file demonstrates numerous tasks all of which use the MQTT agent API
 * to send unique MQTT payloads to unique topics over the same MQTT connection
 * to the same MQTT agent.  Some tasks use QoS0 and others QoS1.
 *
 * Each created task is a unique instance of the task implemented by
 * prvSimpleSubscribePublishTask().  prvSimpleSubscribePublishTask()
 * subscribes to a topic then periodically publishes a message to the same
 * topic to which it has subscribed.  The command context sent to
 * MQTTAgent_Publish() contains a unique number that is sent back to the task
 * as a task notification from the callback function that executes when the
 * PUBLISH operation is acknowledged (or just sent in the case of QoS 0).  The
 * task checks the number it receives from the callback equals the number it
 * previously set in the command context before printing out either a success
 * or failure message.
 */


/* Standard includes. */
#include <string.h>
#include <stdio.h>
#include <assert.h>

/* Kernel includes. */
#include "FreeRTOS.h"
#include "task.h"
#include "queue.h"

/* Demo Specific configs. */
#include "demo_config.h"

/* MQTT library includes. */
#include "core_mqtt.h"

/* MQTT agent include. */
#include "core_mqtt_agent.h"

/* Subscription manager header include. */
#include "subscription_manager.h"

/**
 * @brief This demo uses task notifications to signal tasks from MQTT callback
 * functions.  mqttexampleMS_TO_WAIT_FOR_NOTIFICATION defines the time, in ticks,
 * to wait for such a callback.
 */
#define mqttexampleMS_TO_WAIT_FOR_NOTIFICATION            ( 10000 )

/**
 * @brief Size of statically allocated buffers for holding topic names and
 * payloads.
 */
#define mqttexampleSTRING_BUFFER_LENGTH                   ( 100 )

/**
 * @brief Delay for each task between publishes.
 */
#define mqttexampleDELAY_BETWEEN_PUBLISH_OPERATIONS_MS    ( 1000U )

/**
 * @brief Number of publishes done by each task in this demo.
 */
#define mqttexamplePUBLISH_COUNT                          ( 0xffffffffUL )

/**
 * @brief The maximum amount of time in milliseconds to wait for the commands
 * to be posted to the MQTT agent should the MQTT agent's command queue be full.
 * Tasks wait in the Blocked state, so don't use any CPU time.
 */
#define mqttexampleMAX_COMMAND_SEND_BLOCK_TIME_MS         ( 5000 )

/*-----------------------------------------------------------*/


/*_RB_ TBD comments. */
static const MQTTAgentCommandInfo_t xSynchronousCommandStructure =
{
    /* Set the callback to run when receiving SUBACK to NULL so the
    agent handles this for us. */
    NULL,

    /* The callback is NULL, so the callback context isn't used.  Again,
    the agent will handle this for us. */
    NULL,
    /* The maximum amount of time to wait to write to the MQTT agent
    command queue, if the queue is full.  Then then the maximum time to
    wait for the SUBACK. */

    mqttexampleMAX_COMMAND_SEND_BLOCK_TIME_MS
};

/*-----------------------------------------------------------*/

/**
 * Returns a psuedo random number.  Production code should always use a true
 * random number generator.
 */
extern UBaseType_t uxRand( void );

/**
 * @brief Passed into MQTTAgent_Publish() as the callback to execute when the
 * broker ACKs the PUBLISH message.  Its implementation sends a notification
 * to the task that called MQTTAgent_Publish() to let the task know the
 * PUBLISH operation completed.  It also sets the xReturnStatus of the
 * structure passed in as the command's context to the value of the
 * xReturnStatus parameter - which enables the task to check the status of the
 * operation.
 *
 * See https://freertos.org/mqtt/mqtt-agent-demo.html#example_mqtt_api_call
 *
 * @param[in] pxCommandContext Context of the initial command.
 * @param[in].xReturnStatus The result of the command.
 */
static void prvPublishCommandCallback( MQTTAgentCommandContext_t * pxCommandContext,
                                       MQTTAgentReturnInfo_t * pxReturnInfo );

/**
 * @brief Called by the task to wait for a notification from a callback function
 * after the task first executes either MQTTAgent_Publish()* or
 * MQTTAgent_Subscribe().
 *
 * See https://freertos.org/mqtt/mqtt-agent-demo.html#example_mqtt_api_call
 *
 * @param[in] pxCommandContext Context of the initial command.
 * @param[out] pulNotifiedValue The task's notification value after it receives
 * a notification from the callback.
 *
 * @return pdTRUE if the task received a notification, otherwise pdFALSE.
 */
static BaseType_t prvWaitForCommandAcknowledgment( uint32_t * pulNotifiedValue );

/**
 * @brief Passed into MQTTAgent_Subscribe() as the callback to execute when
 * there is an incoming publish on the topic being subscribed to.  Its
 * implementation just logs information about the incoming publish including
 * the publish messages source topic and payload.
 *
 * See https://freertos.org/mqtt/mqtt-agent-demo.html#example_mqtt_api_call
 *
 * @param[in] pvIncomingPublishCallbackContext Context of the initial command.
 * @param[in] pxPublishInfo Deserialized publish.
 */
static void prvIncomingPublishCallback( void * pvIncomingPublishCallbackContext,
                                        MQTTPublishInfo_t * pxPublishInfo );

/**
 * @brief Subscribe to the topic the demo task will also publish to using the
 * MQTT agent API that doesn't return until receipt of the SUBACK or a
 * timeout.  The demo then publishes to the same topic to which is subscribes,
 * so all outgoing publishes are published back to the task (effectively echoed
 * back).
 *
 * @param[in] xQoS The quality of service (QoS) to use.  Can be zero or one
 * for all MQTT brokers.  Can also be QoS2 if supported by the broker.  AWS IoT
 * does not support QoS2.
 */
static bool prvSubscribeToTopic( MQTTQoS_t xQoS,
                                     char* pcTopicFilter );

/*_RB_ Comments TBD. */
static MQTTStatus_t prvPublishAndWaitAck( const MQTTAgentContext_t * const pxGlobalMqttAgentContext,
                                          MQTTPublishInfo_t *pxPublishInfo,
                                          const MQTTAgentCommandInfo_t * const pxCommandParams );

/**
 * @brief The function that implements the task demonstrated by this file.
 */
static void prvSimpleSubscribePublishTask( void * pvParameters );

/*-----------------------------------------------------------*/

/**
 * @brief The MQTT agent manages the MQTT contexts.  This set the handle to the
 * context used by this demo.
 */
extern MQTTAgentContext_t xGlobalMqttAgentContext;

/*-----------------------------------------------------------*/

/**
 * @brief The buffer to hold the topic filter. The topic is generated at runtime
 * by adding the task names.
 *
 * @note The topic strings must persist until unsubscribed.
 */
static char topicBuf[ democonfigNUM_SIMPLE_SUB_PUB_TASKS_TO_CREATE ][ mqttexampleSTRING_BUFFER_LENGTH ];

/*-----------------------------------------------------------*/

void vStartSimpleSubscribePublishTask( void )
{
    char pcTaskNameBuf[ 15 ];
    uint32_t ulTaskNumber;

    /* Create a few instances of vSimpleSubscribePublishTask(). */
    for( ulTaskNumber = 0; ulTaskNumber < democonfigNUM_SIMPLE_SUB_PUB_TASKS_TO_CREATE; ulTaskNumber++ )
    {
        /* Each instance of prvSimpleSubscribePublishTask() generates a unique name
         * and topic filter for itself from the number passed in as the task
         * parameter. */
        memset( pcTaskNameBuf, 0x00, sizeof( pcTaskNameBuf ) );
        snprintf( pcTaskNameBuf, 10, "SubPub%d", ( int ) ulTaskNumber );

        xTaskCreate( prvSimpleSubscribePublishTask,
                     pcTaskNameBuf,
                     democonfigSIMPLE_SUB_PUB_TASK_STACK_SIZE,
                     ( void * ) ulTaskNumber,
                     tskIDLE_PRIORITY,
                     NULL );
    }
}

/*-----------------------------------------------------------*/

static void prvPublishCommandCallback( MQTTAgentCommandContext_t * pxCommandContext,
                                       MQTTAgentReturnInfo_t * pxReturnInfo )
{
    /* Store the result in the application defined context so the task that
     * initiated the publish can check the operation's status. */
    pxCommandContext->xReturnStatus = pxReturnInfo->returnCode;

    if( pxCommandContext->xTaskToNotify != NULL )
    {
        /* Send the context's ulNotificationValue as the notification value so
         * the receiving task can check the value it set in the context matches
         * the value it receives in the notification. */
        if( xTaskNotify( pxCommandContext->xTaskToNotify,
                     pxCommandContext->ulNotificationValue,
                     eSetValueWithOverwrite ) != pdPASS )
        {
            LogError( ( "-- ERROR -- Task notification failed as notification already pending." ) );
        }
    }
}

/*-----------------------------------------------------------*/

static BaseType_t prvWaitForCommandAcknowledgment( uint32_t * pulNotifiedValue )
{
    BaseType_t xReturn;

    /* Wait for this task to get notified, passing out the value it gets
     * notified with. */
    xReturn = xTaskNotifyWait( 0,
                               0,
                               pulNotifiedValue,
                               pdMS_TO_TICKS( mqttexampleMS_TO_WAIT_FOR_NOTIFICATION ) );
    return xReturn;
}

/*-----------------------------------------------------------*/

static void prvIncomingPublishCallback( void * pvIncomingPublishCallbackContext,
                                        MQTTPublishInfo_t * pxPublishInfo )
{
    static char cTerminatedString[ mqttexampleSTRING_BUFFER_LENGTH ];

    ( void ) pvIncomingPublishCallbackContext;

    /* Create a message that contains the incoming MQTT payload to the logger,
     * terminating the string first. */
    if( pxPublishInfo->payloadLength < mqttexampleSTRING_BUFFER_LENGTH )
    {
        memcpy( ( void * ) cTerminatedString, pxPublishInfo->pPayload, pxPublishInfo->payloadLength );
        cTerminatedString[ pxPublishInfo->payloadLength ] = 0x00;
    }
    else
    {
        memcpy( ( void * ) cTerminatedString, pxPublishInfo->pPayload, mqttexampleSTRING_BUFFER_LENGTH );
        cTerminatedString[ mqttexampleSTRING_BUFFER_LENGTH - 1 ] = 0x00;
    }

    LogInfo( ( "Received incoming publish message \"%s\"", cTerminatedString ) );
}

/*-----------------------------------------------------------*/

static bool prvSubscribeToTopic( MQTTQoS_t xQoS,
                                 char* pcTopicFilter )
{
    MQTTStatus_t xStatus;
    MQTTAgentSubscribeArgs_t xSubscribeArgs;
    MQTTSubscribeInfo_t xSubscribeInfo = { 0 };
    bool xSubscriptionAdded;

    /* Complete the subscribe information.  The topic string must persist for
     * duration of subscription! */
    xSubscribeInfo.pTopicFilter = pcTopicFilter;
    xSubscribeInfo.topicFilterLength = ( uint16_t )strlen( pcTopicFilter );
    xSubscribeInfo.qos = xQoS;

    xSubscribeArgs.pSubscribeInfo = &xSubscribeInfo;
    xSubscribeArgs.numSubscriptions = 1; /*_RB_ Could this be determined inside the MQTT stack?  If not, the name of this structure member needs to make its purpose clearer. */

    LogInfo( ( "Sending subscribe request to agent for topic filter: %s",
               pcTopicFilter ) );

    /* Using xSynchronousCommandStructure, so the MQTT agent will wait for the
     * SUBACK for us.  xSynchronousCommandStructure doesn't provide a callback,
     * so this call to MQTTAgent_Subscribe() won't return until after reception
     * of the SUBACK.  The MQTT agent, and all other tasks, still run as
     * normal though. */
    xStatus = MQTTAgent_Subscribe( &xGlobalMqttAgentContext,
                                   &xSubscribeArgs,
                                   &xSynchronousCommandStructure );

    if( xStatus == MQTTSuccess )
    {
        /* Not an error, but use the error log as the output provides useful
         * information on the demo's progress. */
        LogError( ( "Subscribed to %s", pcTopicFilter ) );

        /* Add subscription so that incoming publishes are routed to the application
         * callback. */
        xSubscriptionAdded = addSubscription( ( SubscriptionElement_t * ) xGlobalMqttAgentContext.pIncomingCallbackContext,
                                              pcTopicFilter,
                                              xSubscribeInfo.topicFilterLength,
                                              prvIncomingPublishCallback,
                                              NULL );

        if( xSubscriptionAdded == false )
        {
            LogError( ( "-- ERROR -- Failed to register an incoming publish callback for topic %.*s.",
                        xSubscribeInfo.topicFilterLength,
                        pcTopicFilter ) );
        }
    }
    else
    {
        LogError( ( "-- ERROR -- Failed to subscribe to %s - bad connection, MQTT agent queue full, or block time too short.",
                   pcTopicFilter ) );
    }

    return xStatus;
}

/*-----------------------------------------------------------*/

static MQTTStatus_t prvPublishAndWaitAck( const MQTTAgentContext_t * const pxGlobalMqttAgentContext,
                                          MQTTPublishInfo_t *pxPublishInfo,
                                          const MQTTAgentCommandInfo_t * const pxCommandParams )
{
    MQTTStatus_t xCommandAdded;

    xCommandAdded = MQTTAgent_Publish( pxGlobalMqttAgentContext,
                                       pxPublishInfo,
                                       pxCommandParams );

    if( xCommandAdded != MQTTSuccess )
    {
        LogError( ( "-- ERROR -- Failed to send publish request to agent - increase either the timeout or the length of the agent's command queue" ) );
    }
#if 0
    else
    {
        /* For QoS 1 and 2, wait for the publish acknowledgment.  For QoS0,
            * wait for the publish to be sent. */
        LogInfo( ( "Task %s waiting for publish %d to complete.",
                   pcTaskName,
                   ulValueToNotify ) );

        prvWaitForCommandAcknowledgment( pulReceivedValue );
    }
#endif
    return xCommandAdded;
}

/*-----------------------------------------------------------*/

static void prvSimpleSubscribePublishTask( void * pvParameters )
{
    MQTTPublishInfo_t xPublishInfo = { 0 };
    char cPayloadBuffer[ mqttexampleSTRING_BUFFER_LENGTH ];
    char *pcTaskName = NULL;;
    MQTTAgentCommandContext_t xCommandContext;
    uint32_t ulNotification = 0U, ulValueToNotify = 0UL;
    MQTTStatus_t xCommandAdded;
    MQTTQoS_t xQoS;
    uint32_t ulTaskNumber = ( uint32_t )pvParameters;
    char * pcTopicBuffer = topicBuf[ ulTaskNumber ];
    TickType_t xTicksToDelay;

    pcTaskName = pcTaskGetName( NULL );

    /* Have different tasks use different QoS.  0 and 1.  2 can also be used
     * if supported by the broker. */
    xQoS = ( MQTTQoS_t ) ( ulTaskNumber % 2UL );
    xQoS = 1;

    /* Create a topic name for this task to publish to. */
    snprintf( pcTopicBuffer, mqttexampleSTRING_BUFFER_LENGTH, "/filter/%s", pcTaskName );

    LogError( ( "Task: %s: ---------STARTING DEMO---------\r\n", pcTaskName ) ); /* Not actually an error, but demo progress logging. */

    /* Subscribe to the same topic to which this task will publish.  That will
     * result in each published message being published from the server back to
     * the target. */
    prvSubscribeToTopic( xQoS, pcTopicBuffer );

    /* Configure the publish operation. */
    memset( ( void * ) &xPublishInfo, 0x00, sizeof( xPublishInfo ) );
    xPublishInfo.qos = xQoS;
    xPublishInfo.pTopicName = pcTopicBuffer;
    xPublishInfo.topicNameLength = ( uint16_t ) strlen( pcTopicBuffer );
    xPublishInfo.pPayload = cPayloadBuffer;

    /* Store the handler to this task in the command context so the callback
     * that executes when the command is acknowledged can send a notification
     * back to this task. */
    memset( ( void * ) &xCommandContext, 0x00, sizeof( xCommandContext ) );
    xCommandContext.xTaskToNotify = xTaskGetCurrentTaskHandle();

    /* For a finite number of publishes... */
    for( ulValueToNotify = 0UL; ulValueToNotify < mqttexamplePUBLISH_COUNT; ulValueToNotify++ )
    {
        /* Create a payload to send with the publish message.  This contains
         * the task name and an incrementing number. */
        snprintf( cPayloadBuffer,
                  mqttexampleSTRING_BUFFER_LENGTH,
                  "%s publishing message %d xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                  pcTaskName,
                  ( int ) ulValueToNotify );

        xPublishInfo.payloadLength = ( uint16_t ) strlen( cPayloadBuffer );

        /* Also store the incrementing number in the command context so it can
         * be accessed by the callback that executes when the publish operation
         * is acknowledged. */
        xCommandContext.ulNotificationValue = ulValueToNotify;

        LogInfo( ( "Sending publish request to agent with message \"%s\" on topic \"%s\"",
                   cPayloadBuffer,
                   pcTopicBuffer ) );

        /* To ensure ulNotification doesn't accidentally hold the expected value
         * as it is to be checked against the value sent from the callback.. */
        ulNotification = ~ulValueToNotify;


        xCommandAdded =  prvPublishAndWaitAck( &xGlobalMqttAgentContext,
                                               &xPublishInfo,
                                               &xSynchronousCommandStructure );

        if( xCommandAdded != MQTTSuccess )
        {
            LogError( ( "-- ERROR -- Failed to send publish request to agent - increase either the timeout or the length of the agent's command queue" ) );
        }
        else
        {
            /* Print out an occasional progress message.  Not actually an
             * error, just useful to see the demo is executing. */
            if( ( ulValueToNotify % 100UL ) == 1UL )
            {
                LogError( ( "Successfully sent and received up to %u", ulValueToNotify ) );
            }

            /* Log statement to indicate successful reception of publish. */
            LogInfo( ( "Task: %s: Short delay before next iteration...", pcTaskName ) );

            /* Add a little randomness into the delay so the tasks don't remain
             * in lockstep. */
            xTicksToDelay = pdMS_TO_TICKS( mqttexampleDELAY_BETWEEN_PUBLISH_OPERATIONS_MS ) +
                            ( uxRand() % 0xff );

//_RB_            vTaskDelay( xTicksToDelay );
        }
    }

    /* Delete the task if it is complete. */
    LogError( ( "-- ERROR -- 'Task %s completed.", pcTaskName ) );
    vTaskDelete( NULL );
}








